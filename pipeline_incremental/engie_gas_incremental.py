"""
ENGIE NL - gas hourly allocation: INCREMENTAL-ONLY pipeline (Lakeflow SDP).

WHAT THIS DOES (in one sentence): every run, find the point-of-delivery (POD)
records that changed since last time, rebuild ONLY those PODs' hourly rows, and
merge them into the big hourly table - inserting new hours, updating changed
ones, and deleting hours that are no longer valid.

History is loaded ONCE by the standalone batch job (init_backfill.py) into the
physical table `hourly_consumption_allocation`. This pipeline never recomputes
history; it only processes the ongoing daily delta and writes into that SAME
table via a Delta sink.

-------------------------------------------------------------------------------
WHY THIS IS PYTHON AND NOT SQL (the maintainability question)
-------------------------------------------------------------------------------
The team's standard flows are SQL (AUTO CDC INTO). This one can't be, for one
reason: RETRACTION. When a POD's validity range shrinks (e.g. valid_to moves
from 9999 to 2026-09-30), or a POD is deleted, the now-invalid hourly rows must
be DELETED from the target. SQL `AUTO CDC INTO` only does keyed upserts - the
orphaned tail hours are simply absent from the new data, so an upsert never
touches them and they linger forever. Deleting them needs a MERGE with
`WHEN NOT MATCHED BY SOURCE ... DELETE`, and a MERGE inside a stream requires
`foreach_batch`, which is Python-only. (Two lesser reasons: writing into a table
this pipeline does not own also needs a Python Delta sink, and the change-feed
bootstrap read below is cleanest in the Python API.)

If ENGIE decides incremental retraction is NOT required - e.g. corrections are
rare and handled by a periodic partition rebuild - then this whole thing becomes
a standard SQL AUTO CDC flow. So "must it be Python?" really means "must we
delete invalid hours incrementally?" That is a functional call, not a technical
one, and is the main thing to confirm with the PO.

-------------------------------------------------------------------------------
HOW IT WORKS - just two objects
-------------------------------------------------------------------------------
1. write_hourly_flow  (@dp.append_flow) - the STREAMING SOURCE.
   Reads the source table's CHANGE DATA FEED, not the table itself. A plain
   streaming read from an empty checkpoint would replay ALL 12M PODs from
   version 0 (~40B exploded rows, ~1h, duplicating the backfill). The change
   feed from the backfill's version returns ONLY the PODs that changed since -
   the small daily delta. It emits just the changed EAN to the sink.

2. write_hourly  (@dp.foreach_batch_sink) - the WRITER.
   For the changed EANs in the batch: re-expand each one's CURRENT record from
   source to hourly grain, then run a scoped MERGE that upserts the valid hours
   and deletes any leftover hours for those EANs (the retraction). The change
   feed already narrowed the work to the changed EANs, so we only ever rebuild a
   handful of PODs per run - never the 12M history.

There is deliberately NO intermediate exploded/deduped streaming table: the sink
rebuilds each changed POD directly from source, so an extra layer would only be
wasted compute. Fewer objects, less to maintain.

-------------------------------------------------------------------------------
CONFIG (set on the pipeline `configuration`)
-------------------------------------------------------------------------------
  engie.source_schema         catalog.schema of the source tables
  engie.target_table          3-part name of the batch-created hourly table
  engie.cdf_starting_version  the version V printed by init_backfill.py (the source
                              version the backfill snapshotted). Used ONLY on the first
                              run to bootstrap; after that the pipeline's own checkpoint
                              drives it. NOTE it is V, not V+1: readChangeFeed fails at
                              startup if startingVersion is beyond the latest commit, and
                              right after a backfill V+1 does not exist yet — so a fresh
                              deploy would crash. Starting at V always works; commit V is
                              re-read once on the first run but the keyed MERGE makes that
                              idempotent (no duplicates). See the reload runbook in the
                              repo README if you ever full-reload history in a new env.
  engie.max_explode_years     forward cap for open-ended ranges (default 10)

Requires delta.enableChangeDataFeed = true on point_of_delivery (see README).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SRC = spark.conf.get("engie.source_schema")
TARGET_TABLE = spark.conf.get("engie.target_table")
MAX_EXPLODE_YEARS = int(spark.conf.get("engie.max_explode_years", "10"))
# The change-feed start version = V, the source version init_backfill.py snapshotted
# for the history (it prints V). The feed is read from V so the stream picks up every
# commit from the backfill point onward. Commit V itself is re-read once on the first
# run, but the keyed MERGE makes that idempotent (no duplicates). NOT V+1: readChangeFeed
# fails at startup if startingVersion is past the latest commit, and right after a
# backfill V+1 does not exist yet. First-run bootstrap only; the checkpoint takes over.
CDF_STARTING_VERSION = spark.conf.get("engie.cdf_starting_version")

POD = f"{SRC}.point_of_delivery"
PROFILE = f"{SRC}.gas_profile_fraction"
SINK = "hourly_delta_sink"
M3_TO_KWH = 9.769

# Keep a gas/PRF row if EITHER its pre- or post-change state is gas/PRF. We do NOT
# filter on _change_type, so the feed carries inserts, updates AND deletes. Because
# the change feed emits the pre-image on updates/deletes, a POD that LEAVES gas
# (gas -> electricity, or a hard delete of a gas row) still appears here via its
# pre-image - so its stale gas hours get retracted. Electricity-only churn is
# excluded, keeping the per-run EAN set small.
GAS_FILTER = "commodity_type ILIKE 'gas' AND allocation_method ILIKE 'PRF'"


# --------------------------------------------------------------------------- #
# OBJECT 1 - the streaming source: the source change feed, changed EANs only.
# --------------------------------------------------------------------------- #
@dp.append_flow(name="write_hourly_flow", target=SINK)
def write_hourly_flow():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .option("startingVersion", CDF_STARTING_VERSION)
        .table(POD)
        .filter(GAS_FILTER)
        .select("point_of_delivery_ean")  # the sink re-reads the full record from source
    )


# --------------------------------------------------------------------------- #
# OBJECT 2 - the writer: re-expand changed EANs, scoped retracting MERGE.
# --------------------------------------------------------------------------- #
@dp.foreach_batch_sink(name=SINK)
def write_hourly(batch_df, batch_id):
    from pyspark.sql.window import Window
    spark = batch_df.sparkSession  # use the batch's session, not module-level spark

    # The distinct EANs that changed this run. Collected to a small Python list -
    # used both to scope the source re-expansion and (as a literal list) to scope
    # the MERGE's delete. Daily deltas are small, so this list is short.
    changed_eans = [r["point_of_delivery_ean"]
                    for r in batch_df.select("point_of_delivery_ean").distinct().collect()]
    if not changed_eans:
        return  # nothing changed this trigger
    changed = spark.createDataFrame([(e,) for e in changed_eans], ["point_of_delivery_ean"])

    # Re-expand each changed EAN's CURRENT record from source (a static read of the
    # latest table state - NOT the change feed). A deleted EAN has no current record,
    # so it produces zero rows here, and the MERGE below removes all its hours.
    hi_cap = F.expr(f"date_add(current_date(), {MAX_EXPLODE_YEARS} * 365)")
    pod = (
        spark.read.table(POD)
        .filter(GAS_FILTER)
        .join(changed, "point_of_delivery_ean", "inner")
    )
    # DEDUP AT SOURCE-RECORD GRAIN *BEFORE* EXPLODE - keep the latest __record_timestamp
    # record per (ean, category). Essential and different from per-day dedup: when a new
    # record supersedes an old one (e.g. valid_to 9999 -> 2026-09-30), only the new range
    # must expand. Per-day dedup after explode would UNION old and new ranges and leave
    # the dropped tail behind.
    wr = Window.partitionBy("point_of_delivery_ean", "profile_category_code") \
               .orderBy(F.col("__record_timestamp").desc())
    pod = pod.withColumn("_rr", F.row_number().over(wr)).filter("_rr = 1").drop("_rr")

    daily = (
        pod.select(
            "point_of_delivery_ean", "profile_category_code", "sja",
            "sj_unit_of_measure", "__record_timestamp",
            F.explode(F.sequence(F.col("effective_from_date"),
                                 F.least(F.col("effective_to_date"), hi_cap),
                                 F.expr("INTERVAL 1 DAY"))).alias("supply_date"),
        )
        .withColumn("supply_year", F.year("supply_date"))
        .withColumn("supply_month", F.month("supply_date"))
    )

    pf = F.broadcast(spark.read.table(PROFILE))  # small dimension - broadcast it
    current = daily.join(
        pf,
        (daily.profile_category_code == pf.profile_category_code)
        & (daily.supply_date == pf.supply_date),
    ).select(
        daily.point_of_delivery_ean, daily.profile_category_code, daily.sja,
        daily.sj_unit_of_measure,
        pf.supply_start_date_time_utc,
        (pf.supply_start_date_time_utc + F.expr("INTERVAL 1 HOUR")).alias("supply_end_date_time_utc"),
        pf.supply_hour, pf.profile_fraction,
        (daily.sja * pf.profile_fraction).cast("decimal(38,6)").alias("offtake_volume_m3"),
        (daily.sja * pf.profile_fraction * F.lit(M3_TO_KWH)).cast("decimal(38,6)").alias("offtake_volume_kwh"),
        daily.supply_year, daily.supply_month, daily.__record_timestamp,
    )
    current.createOrReplaceTempView("_current_expansion")

    # Scoped retracting MERGE:
    #  - upsert the current valid hours for the changed EANs;
    #  - DELETE any target hour for a CHANGED EAN that is not in the current expansion
    #    (= the dropped tail of a shrunk range, or every hour of a hard-deleted EAN).
    # The delete scope is a LITERAL list of this batch's changed EANs - Delta MERGE
    # forbids a subquery in the NOT-MATCHED-BY-SOURCE condition.
    scope = ",".join("'" + e.replace("'", "''") + "'" for e in changed_eans)
    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} t
        USING _current_expansion s
        ON  t.point_of_delivery_ean = s.point_of_delivery_ean
        AND t.profile_category_code = s.profile_category_code
        AND t.supply_start_date_time_utc = s.supply_start_date_time_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE
             AND t.point_of_delivery_ean IN ({scope})
             THEN DELETE
    """)
