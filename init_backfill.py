#!/usr/bin/env python3
"""
ENGIE NL — gas hourly allocation: STANDALONE BATCH INIT (Option B).

Runs OUTSIDE any pipeline, on Databricks Connect serverless. Builds all history
into the physical target table `hourly_consumption_allocation`, chunked by year
(fast batch path: explode + window dedup + broadcast fan-out, no streaming MERGE).

The incremental SDP pipeline (engie_gas_incremental.py) then APPENDS forward data
into this SAME table via a Delta sink — so there is ONE physical table, and the
pipeline never re-computes history.

The seam is made exact with a PINNED Delta version:
  * We read the current version V of point_of_delivery and process `VERSION AS OF V`.
  * This script prints V. Configure the incremental pipeline with
    engie.cdf_starting_version = V.
  * Why V and NOT V+1: readChangeFeed with a startingVersion beyond the table's
    latest commit FAILS at query start ("cannot time travel to version N") — it does
    not wait. Right after a backfill, V+1 does not exist yet, so a freshly-deployed
    pipeline would crash until the first source change. Starting at V (which always
    exists) avoids that. The one commit at V gets re-read on the first run, but the
    target write is an idempotent keyed MERGE, so those rows merge to identical keys —
    no duplicates, no double counting. After the first checkpoint the stream moves
    forward and never re-reads V.

Usage:
  DATABRICKS_CONFIG_PROFILE=... python init_backfill.py \
      --catalog alp_serverless_internal_ws_catalog --schema engie_nl_optb \
      --source-schema alp_serverless_internal_ws_catalog.engie_nl_data \
      --years 2014,2015 [--truncate]
"""
from __future__ import annotations
import argparse, os, sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window

M3_TO_KWH = 9.769
MAX_EXPLODE_YEARS = 10
TARGET = "hourly_consumption_allocation"

TARGET_DDL = """
CREATE TABLE IF NOT EXISTS {tbl} (
    point_of_delivery_ean      STRING,
    profile_category_code      STRING,
    sja                        DECIMAL(18,3),
    sj_unit_of_measure         STRING,
    supply_start_date_time_utc TIMESTAMP,
    supply_end_date_time_utc   TIMESTAMP,
    supply_hour                INT,
    profile_fraction           DECIMAL(36,8),
    offtake_volume_m3          DECIMAL(38,6),
    offtake_volume_kwh         DECIMAL(38,6),
    supply_year                INT,
    supply_month               INT,
    __record_timestamp         TIMESTAMP
)
USING delta
PARTITIONED BY (supply_year, supply_month)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true',
               'delta.autoOptimize.autoCompact'='true')
"""


def current_version(spark, table: str) -> int:
    v = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"]
    return int(v)


def build_year(spark, pod_snap, profile_tbl, year: int):
    lo, hi = f"{year}-01-01", f"{year}-12-31"
    hi_cap = F.expr(f"date_add(current_date(), {MAX_EXPLODE_YEARS} * 365)")

    pod = pod_snap.filter(
        "commodity_type ILIKE 'gas' AND allocation_method ILIKE 'PRF'"
    ).filter(f"effective_from_date <= DATE'{hi}' AND effective_to_date >= DATE'{lo}'")

    start = F.greatest(F.col("effective_from_date"), F.lit(lo).cast("date"))
    end = F.least(F.col("effective_to_date"), hi_cap, F.lit(hi).cast("date"))

    daily = (
        pod.select(
            "point_of_delivery_ean", "profile_category_code", "sja",
            "sj_unit_of_measure", "__record_timestamp",
            F.explode(F.sequence(start, end, F.expr("INTERVAL 1 DAY"))).alias("supply_date"),
        )
        .withColumn("supply_year", F.year("supply_date"))
        .withColumn("supply_month", F.month("supply_date"))
    )
    # batch window dedup at daily grain — latest __record_timestamp wins
    w = Window.partitionBy(
        "point_of_delivery_ean", "profile_category_code", "supply_date"
    ).orderBy(F.col("__record_timestamp").desc())
    daily = daily.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

    pf = F.broadcast(spark.read.table(profile_tbl))
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


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--source-schema", required=True, help="catalog.schema of source tables")
    p.add_argument("--years", required=True, help="comma list e.g. 2014,2015")
    p.add_argument("--truncate", action="store_true", help="TRUNCATE target before load")
    args = p.parse_args()

    years = [int(y) for y in args.years.split(",") if y.strip()]
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

    tgt = f"{args.catalog}.{args.schema}.{TARGET}"
    pod_tbl = f"{args.source_schema}.point_of_delivery"
    profile_tbl = f"{args.source_schema}.gas_profile_fraction"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    spark.sql(TARGET_DDL.format(tbl=tgt))
    if args.truncate:
        spark.sql(f"TRUNCATE TABLE {tgt}")

    v = current_version(spark, pod_tbl)
    print(f"📌 Pinned source version V = {v}")
    print(f"   -> configure the incremental pipeline with engie.cdf_starting_version = {v}")

    pod_snap = spark.read.option("versionAsOf", v).table(pod_tbl)

    for y in years:
        df = build_year(spark, pod_snap, profile_tbl, y)
        df.write.mode("append").saveAsTable(tgt)
        n = spark.table(tgt).filter(F.col("supply_year") == y).count()
        print(f"  ✓ {y}: appended (year now has {n:,} rows in {tgt})")

    print(f"\nDone. Target = {tgt}")
    print(f"Next: run the incremental pipeline with engie.cdf_starting_version={v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
