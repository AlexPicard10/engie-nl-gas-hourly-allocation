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

Flow:
  pod_daily_stream (view)  -> readChangeFeed(startingVersion=V) → filter to new/updated
                              rows → explode the delta to daily grain
  pod_daily_deduped (ST)   -> AUTO CDC SCD-1 dedup of the delta at (ean,category,day)
  hourly_delta_sink        -> the externally-managed hourly_consumption_allocation
  write_hourly (append flow) -> stream deduped daily, broadcast fan-out, into the sink

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
# Layer 2 — Delta sink to the batch-owned physical table + append flow fan-out.
# --------------------------------------------------------------------------- #
dp.create_sink(
    name=SINK,
    format="delta",
    options={"tableName": TARGET_TABLE},
)


@dp.append_flow(name="write_hourly", target=SINK)
def write_hourly():
    daily = spark.readStream.option("skipChangeCommits", "true").table(DAILY)
    # Profiles are ~316k rows / a few MB — broadcasting the whole table is already
    # trivially cheap, and `daily` is now only the delta (from the change feed), so
    # each microbatch's join touches only the delta's rows. Restricting the profile
    # scan by delta-date would add a stateful dependency for ~zero gain; broadcast
    # of the full small dimension is the correct, simplest choice.
    pf = F.broadcast(spark.read.table(PROFILE))
    return daily.join(
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
