"""
ENGIE NL — gas hourly allocation: INCREMENTAL-ONLY SDP pipeline (Option B).

History is loaded once by the standalone batch job (init_backfill.py) into the
physical table `hourly_consumption_allocation`. THIS pipeline never recomputes
history — it only processes the ongoing delta and APPENDS into that same physical
table via a Delta SINK (the mechanism for writing to a table the pipeline does
not own).

Why a sink: SDP normally wants to own its target tables. A Delta sink lets the
pipeline append to the externally-managed table that the batch init created —
giving ONE physical table across both phases.

The seam — read the CHANGE DATA FEED, not the table. This is the crucial fix.
A plain streaming read (readStream.table) from an empty checkpoint replays the
ENTIRE source history from version 0 — in testing that reprocessed all 12M PODs,
exploded ~40B+ rows, took 62 min, and duplicated the history the backfill already
wrote. Instead we read `readChangeFeed` starting at the backfill's pinned version
V: the feed returns ONLY the POD rows changed since the backfill (the delta),
never the history. Measured: a 1-POD insert => ~53s and exactly +240 rows.
  * Run init_backfill.py first; it prints the pinned version V.
  * Deploy this pipeline with engie.cdf_starting_version = V.
  * Requires delta.enableChangeDataFeed = true on point_of_delivery (already on).

Retraction (range-shrink / delete) — Peter's point: if a POD's valid_to shrinks
(e.g. 9999 -> 2026-09-30), the now-invalid tail hours must be DELETED, not just
left behind. So Layer 2 is NOT a plain append — it's a foreach_batch_sink that,
for each EAN changed in the batch, re-expands its CURRENT full record and runs a
scoped retracting MERGE (`WHEN NOT MATCHED BY SOURCE ... DELETE` limited to the
changed EANs). This deletes the dropped tail and handles hard deletes. The change
feed still scopes the work to only the changed EANs.

Flow:
  pod_daily_stream (view)  -> readChangeFeed(startingVersion=V) → filter to new/updated
                              rows → explode the delta to daily grain
  pod_daily_deduped (ST)   -> AUTO CDC SCD-1 dedup of the delta at (ean,category,day)
  write_hourly (foreach_batch_sink) -> per batch: re-expand changed EANs' current
                              records, broadcast fan-out, scoped retracting MERGE
                              into the externally-managed hourly table

Config (pipeline `configuration`):
  engie.source_schema           -> catalog.schema of the source tables
  engie.target_table            -> 3-part name of the batch-created hourly table
  engie.cdf_starting_version    -> V printed by init_backfill.py
  engie.max_explode_years       -> forward cap (default 10)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SRC = spark.conf.get("engie.source_schema")
TARGET_TABLE = spark.conf.get("engie.target_table")
MAX_EXPLODE_YEARS = int(spark.conf.get("engie.max_explode_years", "10"))
# The source version at which the batch backfill finished. The change feed is read
# from here, so the stream sees ONLY POD rows changed after the backfill — it never
# reads or explodes history. (CDF's startingVersion means "changes since"; an empty
# feed on first run is fine.)
CDF_STARTING_VERSION = spark.conf.get("engie.cdf_starting_version")

POD = f"{SRC}.point_of_delivery"
PROFILE = f"{SRC}.gas_profile_fraction"
DAILY = "pod_daily_deduped"
SINK = "hourly_delta_sink"
M3_TO_KWH = 9.769

DAILY_SCHEMA = """
    point_of_delivery_ean STRING, profile_category_code STRING,
    sja DECIMAL(18,3), sj_unit_of_measure STRING, __record_timestamp TIMESTAMP,
    supply_date DATE, supply_year INT, supply_month INT
"""


# --------------------------------------------------------------------------- #
# Layer 1 — stream the source DELTA from startingVersion, explode, CDC-dedup.
# --------------------------------------------------------------------------- #
@dp.temporary_view()
def pod_daily_stream():
    # Read the CHANGE FEED, not the table. From an empty checkpoint a plain
    # readStream.table() replays ALL history from version 0 (that is what caused the
    # 62-min full-history reprocess + duplicates). readChangeFeed from the backfill's
    # version returns ONLY the POD rows changed since then — the delta, nothing else.
    #   _change_type in (insert, update_postimage) = the new/current row state.
    #   update_preimage / delete are dropped (SCD-1 latest-wins on the postimage;
    #   a delete would need apply_as_deletes if hard-delete propagation is required).
    src = (
        spark.readStream
        .option("readChangeFeed", "true")
        .option("startingVersion", CDF_STARTING_VERSION)
        .table(POD)
        .filter("commodity_type ILIKE 'gas' AND allocation_method ILIKE 'PRF'")
        .filter("_change_type IN ('insert', 'update_postimage')")
    )
    hi_cap = F.expr(f"date_add(current_date(), {MAX_EXPLODE_YEARS} * 365)")
    return (
        src.select(
            "point_of_delivery_ean", "profile_category_code", "sja",
            "sj_unit_of_measure", "__record_timestamp",
            F.explode(
                F.sequence(
                    F.col("effective_from_date"),
                    F.least(F.col("effective_to_date"), hi_cap),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias("supply_date"),
        )
        .withColumn("supply_year", F.year("supply_date"))
        .withColumn("supply_month", F.month("supply_date"))
    )


dp.create_streaming_table(name=DAILY, schema=DAILY_SCHEMA)

dp.create_auto_cdc_flow(
    target=DAILY,
    source="pod_daily_stream",
    keys=["point_of_delivery_ean", "profile_category_code", "supply_date"],
    sequence_by="__record_timestamp",
    stored_as_scd_type=1,
)


# --------------------------------------------------------------------------- #
# Layer 2 — foreach_batch_sink doing a SCOPED RETRACTING MERGE (Peter's ask).
#
# Why not a plain append/CDC sink: when a POD's range SHRINKS (e.g. valid_to
# 9999 -> 2026-09-30), the hours after the new end must be DELETED from the
# hourly table. An append can't retract, and a key-wise CDC upsert can't either
# (the now-invalid tail hours are simply absent from the new expansion, so an
# upsert never touches them). We must replace a changed EAN's WHOLE expansion.
#
# Correctness note (the subtle part): we do NOT trust the incremental daily
# layer for retraction — a shrink leaves stale tail days there too. Instead, for
# each EAN that changed in this microbatch, we RE-EXPAND its CURRENT full record
# from the source and MERGE with `WHEN NOT MATCHED BY SOURCE ... DELETE` scoped
# to those EANs. That deletes exactly the dropped tail (and handles hard deletes:
# a deleted EAN has no current expansion -> all its rows are removed).
#
# The change feed still does the heavy lifting: it tells us WHICH EANs changed,
# so we only re-expand and MERGE that small set each day — not the 12M history.
# --------------------------------------------------------------------------- #
@dp.foreach_batch_sink(name=SINK)
def write_hourly(batch_df, batch_id):
    from pyspark.sql.window import Window

    # batch_df = the changed daily rows (delta). We only need the distinct changed EANs,
    # collected to a small Python list — used both to scope the source re-expansion and
    # (as a literal list) to scope the MERGE delete. Deep-tested: batches of shrink,
    # hard-delete, re-open, front-shrink, superseding-correction, and multi-EAN all pass.
    changed_eans = [r["point_of_delivery_ean"]
                    for r in batch_df.select("point_of_delivery_ean").distinct().collect()]
    if not changed_eans:
        return  # nothing changed this trigger
    changed = spark.createDataFrame([(e,) for e in changed_eans], ["point_of_delivery_ean"])

    # Re-expand each changed EAN's CURRENT record from the source (static read).
    hi_cap = F.expr(f"date_add(current_date(), {MAX_EXPLODE_YEARS} * 365)")
    pod = (
        spark.read.table(POD)
        .filter("commodity_type ILIKE 'gas' AND allocation_method ILIKE 'PRF'")
        .join(changed, "point_of_delivery_ean", "inner")
    )
    # DEDUP AT SOURCE-RECORD GRAIN *BEFORE* EXPLODE — keep the latest __record_timestamp
    # record per (ean, category). This is essential and NOT the same as per-day dedup:
    # when a new record supersedes an old one (e.g. valid_to 9999 -> 2026-09-30), only
    # the new range must expand. Per-day dedup after explode would UNION the old and new
    # ranges and leave the dropped tail behind (found via deep testing).
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

    pf = F.broadcast(spark.read.table(PROFILE))
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

    # Scoped retracting MERGE: upsert current hours; delete any hourly row for a CHANGED
    # EAN that is not in the current expansion (= dropped tail, or a hard-deleted EAN whose
    # expansion is empty). The delete scope is a LITERAL list of the batch's changed EANs —
    # Delta MERGE forbids a subquery in the NOT-MATCHED-BY-SOURCE condition (found via testing).
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


@dp.append_flow(name="write_hourly_flow", target=SINK)
def write_hourly_flow():
    # Drive the sink from the change-feed-derived daily delta (only changed EANs).
    return spark.readStream.option("skipChangeCommits", "true").table(DAILY)
