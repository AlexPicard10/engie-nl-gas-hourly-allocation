#!/usr/bin/env python3
"""
Generate synthetic source data to reproduce ENGIE Energie Nederland's
range-join / fan-out performance problem (GAS profile allocation).

See PROBLEM_CONTEXT.md for the full brief. Schema and join semantics below are
aligned to Peter's actual queries (Aug 2026 feedback).

Two COMPACT source tables are generated:

  Table 1  point_of_delivery            (~12M rows at full scale) — COMPACT
           one row = a point-of-delivery (POD, Dutch EAN) + its attributes valid
           for a date RANGE [effective_from_date, effective_to_date]. Ranges can
           be 1 day .. ~10y, can overlap for the same EAN, extend into past/future.
           Carries `sja` (standard annual consumption) which drives offtake volume,
           plus the `commodity_type` (gas) and `allocation_method` (PRF) filters and
           the `__record_timestamp` CDC sequence column.

  Table 2  gas_profile_fraction         (~316k rows at full scale) — hourly
           standard gas load-profile curves: for a profile_category_code, the
           fraction of annual consumption attributed to each specific hour.

The PROBLEM is NOT these tables (both are small). The problem is the downstream
pipeline that (1) explodes each POD range to daily grain (12M -> ~2.8B) and
(2) joins the hourly profile fractions on category + calendar date, fanning out to
~40B rows (one row per POD per hour of its validity). This script only builds the
two compact SOURCE tables. The exploded daily table and the final POD x hour table
are produced by the pipeline you are trying to optimize.

Join Peter is reproducing (compact form, with the range join):

    select pod.point_of_delivery_ean, pod.profile_category_code, pod.sja,
           pf.supply_start_date_time_utc, pf.profile_fraction,
           pod.sja * pf.profile_fraction as offtake_volume_m3, ...
    from point_of_delivery pod
    join gas_profile_fraction pf
      on pod.profile_category_code = pf.profile_category_code
     and cast(from_utc_timestamp(pf.supply_start_date_time_utc,'Europe/Amsterdam') as date)
             between pod.effective_from_date and pod.effective_to_date
    where pod.commodity_type ilike 'gas' and pod.allocation_method ilike 'PRF'

Everything is tunable via a single SCALE factor plus the constants below, so you
can start at 1% and scale up.

Usage
-----
  python generate_engie_nl_data.py --catalog my_catalog --schema engie_nl_data --scale 0.01
  DATABRICKS_CATALOG=my_catalog DATABRICKS_SCHEMA=engie_nl_data python generate_engie_nl_data.py --scale 0.01

Requirements
------------
  Python 3.12, databricks-connect>=16.4 (serverless). Native Spark expressions
  only (fast, fully distributed).
      uv pip install "databricks-connect>=16.4,<17.4"
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import functions as F

# --------------------------------------------------------------------------- #
# Table names (Peter's real names — keeps the reproduce query verbatim).
# --------------------------------------------------------------------------- #
POD_TABLE = "point_of_delivery"
PROFILE_TABLE = "gas_profile_fraction"

# --------------------------------------------------------------------------- #
# Configuration — the knobs that control scale and fan-out.
# --------------------------------------------------------------------------- #

# Full-scale row counts (multiplied by --scale). Defaults match the real case.
FULL_SCALE_POD_ROWS = 12_000_000        # point_of_delivery rows
FULL_SCALE_POD_DISTINCT = 3_000_000     # distinct PODs (=> ~4 history rows/POD)

# gas_profile_fraction is the SMALL dimension. Kept full size regardless of
# --scale so per-POD fan-out (the join blow-up) stays representative.
# ~316k rows = 3 gas categories x hourly over 12 years:  3 x 12 x 8760 = 315,360.
GAS_CATEGORY_CODES = ["G1A", "G2A", "G2C"]   # standard Dutch gas profile categories
PROFILE_SPAN_START = "2014-01-01"            # profiles cover [start, start + span_years)
PROFILE_SPAN_YEARS = 12                      # through 2025 — covers most POD ranges

# POD validity ranges are drawn to overlap the profile span. effective_from lands
# in this window; long ranges can run past the profile span (unmatched tail, realistic).
POD_EFFECTIVE_FROM_START = "2014-01-01"
POD_EFFECTIVE_FROM_END = "2024-01-01"

# Range-length distribution: (weight, min_days, max_days). Skewed — most ranges
# are short, a long tail runs up to ~10 years. The weighted mean drives the
# explode size (daily rows ≈ effective_POD_rows * mean_range_days).
RANGE_BUCKETS = [
    (0.72, 1, 60),        # short: activations/switches within ~2 months
    (0.18, 60, 730),      # medium: months to ~2 years
    (0.07, 730, 2555),    # long: ~2 to 7 years
    (0.03, 2555, 3650),   # very long: up to ~10 years
]

# FK skew: which gas profile category a POD row references (80/20 style).
CATEGORY_WEIGHTS = [0.55, 0.30, 0.15]

# Source-row filters exercised by the pipeline (kept representative, not uniform).
COMMODITY_WEIGHTS = {"gas": 0.85, "electricity": 0.15}   # WHERE commodity_type ilike 'gas'
ALLOCATION_WEIGHTS = {"PRF": 0.80, "AMR": 0.20}          # WHERE allocation_method ilike 'PRF'

# sja = standard annual consumption (m3/yr). Log-normal via exp(mu + sigma*N(0,1)).
# Dutch household gas ~1200-1500 m3/yr with a long tail for larger connections.
SJA_LOG_MEAN = 7.09    # exp(7.09) ~ 1200 m3 median
SJA_LOG_SIGMA = 0.60
SJA_MIN = 100.0

MAX_EXPLODE_YEARS = 10  # matches the pipeline's `current_date() + interval 10 years` cap
SEED = 42


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _enable_cdf(spark, table: str):
    """Turn on Delta Change Data Feed so the incremental pipeline can read only
    changed rows (INSERT/UPDATE/DELETE) from these sources."""
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


def stable_unif(key_col, salt: int):
    """Deterministic uniform in [0,1) keyed on `key_col` — same key => same value.
    Used so a POD's physical attributes stay identical across its history rows."""
    return (F.abs(F.hash(key_col, F.lit(salt))) % F.lit(1_000_003)) / F.lit(1_000_003.0)


def stable_normal(key_col, salt: int):
    """Deterministic ~N(0,1) keyed on `key_col`, via central-limit of 3 stable
    uniforms (mean 1.5, std 0.5). Approximate but stable and fine for synthetic sja."""
    u1 = stable_unif(key_col, salt)
    u2 = stable_unif(key_col, salt + 7919)
    u3 = stable_unif(key_col, salt + 104729)
    return (u1 + u2 + u3 - F.lit(1.5)) / F.lit(0.5)


def num_partitions(n_rows: int) -> int:
    if n_rows < 100_000:
        return 8
    if n_rows < 500_000:
        return 16
    if n_rows < 1_000_000:
        return 32
    if n_rows < 50_000_000:
        return 64
    return 256


def weighted_pick(rand_col, choices: list, weights: list):
    """Nested-when Spark expression picking `choices` by `weights` (normalized)."""
    norm_total = sum(weights[: len(choices)])
    norm = [w / norm_total for w in weights[: len(choices)]]
    thresholds, acc = [], 0.0
    for w in norm:
        acc += w
        thresholds.append(acc)
    expr = F.lit(choices[-1])
    for i in range(len(choices) - 2, -1, -1):
        expr = F.when(rand_col < F.lit(thresholds[i]), F.lit(choices[i])).otherwise(expr)
    return expr


def range_days_expr(bucket_rand, within_rand):
    """Nested-when expression that samples a range length from RANGE_BUCKETS."""
    total_w = sum(b[0] for b in RANGE_BUCKETS)
    thresholds, acc = [], 0.0
    for w, _, _ in RANGE_BUCKETS:
        acc += w / total_w
        thresholds.append(acc)
    _, lo_last, hi_last = RANGE_BUCKETS[-1]
    expr = (F.lit(lo_last) + F.floor(within_rand * F.lit(hi_last - lo_last))).cast("int")
    for i in range(len(RANGE_BUCKETS) - 2, -1, -1):
        _, lo, hi = RANGE_BUCKETS[i]
        val = (F.lit(lo) + F.floor(within_rand * F.lit(hi - lo))).cast("int")
        expr = F.when(bucket_rand < F.lit(thresholds[i]), val).otherwise(expr)
    return expr


def mean_range_days() -> float:
    total_w = sum(b[0] for b in RANGE_BUCKETS)
    return sum(w / total_w * (lo + hi) / 2 for w, lo, hi in RANGE_BUCKETS)


# --------------------------------------------------------------------------- #
# Generators
# --------------------------------------------------------------------------- #

def generate_profiles(spark, catalog: str, schema: str):
    """
    gas_profile_fraction: the gas categories x hourly timestamps over the profile
    span. Small dimension, full size regardless of --scale. `profile_fraction` is
    the share of ANNUAL consumption allocated to each hour, so it sums to ~1.0 per
    category per year (sja * profile_fraction = that hour's offtake volume in m3).
    """
    codes = GAS_CATEGORY_CODES
    n_hours = PROFILE_SPAN_YEARS * 365 * 24
    total_rows = n_hours * len(codes)

    hours = spark.range(0, n_hours, numPartitions=num_partitions(total_rows)).select(
        F.expr(f"timestampadd(HOUR, id, to_timestamp('{PROFILE_SPAN_START} 00:00:00'))")
        .alias("supply_start_date_time_utc")
    )
    cats = hours.select(
        "*",
        F.explode(F.array(*[F.lit(c) for c in codes])).alias("profile_category_code"),
    )

    hour_of_day = F.hour("supply_start_date_time_utc")
    day_of_year = F.dayofyear("supply_start_date_time_utc")
    cat_offset = (F.abs(F.hash("profile_category_code")) % F.lit(5)).cast("double")

    # Gas load shape: strong seasonal (heating) weight + diurnal morning/evening
    # peaks. Normalized so the mean hourly fraction ≈ 1 / hours_per_year, i.e. the
    # yearly sum per category ≈ 1.0 (a share of annual consumption).
    seasonal = F.lit(1.0) + F.lit(0.8) * F.cos(
        F.lit(2 * 3.141592653589793) * (day_of_year - F.lit(15)) / F.lit(365.0)
    )  # peaks in winter
    diurnal = (
        F.lit(1.0)
        + F.lit(0.5) * F.exp(-F.pow(hour_of_day - (F.lit(7) + cat_offset * 0.2), 2) / F.lit(6.0))
        + F.lit(0.6) * F.exp(-F.pow(hour_of_day - F.lit(19), 2) / F.lit(6.0))
    )
    shape = seasonal * diurnal
    # Empirically calibrated so sum(profile_fraction) per category-year ≈ 1.0
    # (i.e. fractions are a share of ANNUAL consumption: sja * sum(fraction) ≈ sja).
    mean_shape = 1.20  # ~E[seasonal]*E[diurnal] measured against this shape
    fraction = shape / (F.lit(mean_shape) * F.lit(float(n_hours) / PROFILE_SPAN_YEARS))

    realized_temp = (
        F.lit(10.5)
        + F.lit(8.5) * F.sin(F.lit(2 * 3.141592653589793) * (day_of_year - F.lit(100)) / F.lit(365.0))
        + (F.rand(SEED + 7) - F.lit(0.5)) * F.lit(3.0)
    )

    profiles = cats.select(
        F.col("profile_category_code"),
        F.col("supply_start_date_time_utc"),
        fraction.cast("decimal(36,8)").alias("profile_fraction"),
        (fraction * F.lit(1.08)).cast("decimal(36,8)").alias("profile_fraction_top"),
        (fraction * (F.lit(1.0) + F.greatest(F.lit(0.0), (F.lit(15.0) - realized_temp)) * F.lit(0.01)))
        .cast("decimal(36,8)")
        .alias("profile_fraction_rer"),
        F.lit(15.0).cast("decimal(36,8)").alias("rer_temperature_threshold_celcius"),
        realized_temp.cast("decimal(36,8)").alias("realized_temperature"),
        # Calendar helper columns the exploded-join equi-joins on (year/month/date).
        F.year("supply_start_date_time_utc").alias("supply_year"),
        F.month("supply_start_date_time_utc").alias("supply_month"),
        F.to_date("supply_start_date_time_utc").alias("supply_date"),
        # Hour-of-day (0-23) — useful as a distribution / clustering key.
        F.hour("supply_start_date_time_utc").alias("supply_hour"),
    )

    table = f"{catalog}.{schema}.{PROFILE_TABLE}"
    profiles.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    _enable_cdf(spark, table)
    print(f"  ✓ {table}: {total_rows:,} rows ({len(codes)} gas categories x {n_hours:,} hourly steps) [CDF on]")


def generate_pod(spark, catalog: str, schema: str, scale: float):
    """
    point_of_delivery: POD (EAN) + validity range + the gas profile category it
    maps to + sja (annual consumption) + the commodity/allocation filter columns +
    the __record_timestamp CDC sequence column. Multiple rows share an EAN
    (attribute history); ranges can overlap.
    """
    n_rows = max(1, int(FULL_SCALE_POD_ROWS * scale))
    n_distinct = max(1, int(FULL_SCALE_POD_DISTINCT * scale))
    codes = GAS_CATEGORY_CODES

    # Row-level random draws — these vary per history row of the same EAN.
    #   u_from/u_bucket/u_within -> validity range (a POD's history rows differ in time)
    #   u_rec                    -> __record_timestamp (CDC sequence)
    # rand() is nondeterministic, so materialize the draws as stable columns first
    # (otherwise they'd be re-rolled inside each nested when()).
    df = (
        spark.range(0, n_rows, numPartitions=num_partitions(n_rows))
        .withColumn("u_from", F.rand(SEED + 1))
        .withColumn("u_bucket", F.rand(SEED + 2))
        .withColumn("u_within", F.rand(SEED + 3))
        .withColumn("u_rec", F.rand(SEED + 9))
    )

    # EAN = the physical connection. Reusing the index pool (< n_rows) gives repeated
    # EANs = attribute history per POD.
    pod_idx = (F.abs(F.hash(F.col("id"), F.lit(SEED))) % F.lit(n_distinct)).cast("long")
    ean = F.concat(F.lit("871"), F.lpad((pod_idx + F.lit(100_000_000_000_000)).cast("string"), 15, "0"))

    # ---- Per-EAN attributes: derived deterministically from the EAN, so every
    # history row of the same POD carries the SAME commodity/category/sja/allocation
    # (physical properties of the connection, not per-row noise). Only the validity
    # dates and __record_timestamp change across a POD's history. ----
    category = weighted_pick(stable_unif(ean, SEED + 40), codes, CATEGORY_WEIGHTS)
    commodity = weighted_pick(stable_unif(ean, SEED + 60), list(COMMODITY_WEIGHTS), list(COMMODITY_WEIGHTS.values()))
    allocation = weighted_pick(stable_unif(ean, SEED + 80), list(ALLOCATION_WEIGHTS), list(ALLOCATION_WEIGHTS.values()))
    sja = F.greatest(
        F.lit(SJA_MIN),
        F.exp(F.lit(SJA_LOG_MEAN) + F.lit(SJA_LOG_SIGMA) * stable_normal(ean, SEED + 50)),
    ).cast("decimal(18,3)")

    # ---- Per-row attributes: the validity range and CDC timestamp vary per row. ----
    from_start = F.to_date(F.lit(POD_EFFECTIVE_FROM_START))
    window_days = F.datediff(F.to_date(F.lit(POD_EFFECTIVE_FROM_END)), from_start)
    effective_from = F.date_add(from_start, (F.col("u_from") * window_days).cast("int"))
    range_days = range_days_expr(F.col("u_bucket"), F.col("u_within"))
    # __record_timestamp: CDC sequence — an ingestion timestamp over a ~2y window.
    rec_ts = F.expr("timestampadd(SECOND, cast(u_rec * 63072000 as int), to_timestamp('2023-01-01 00:00:00'))")

    pod = (
        df.select(
            ean.alias("point_of_delivery_ean"),
            category.alias("profile_category_code"),
            sja.alias("sja"),
            F.lit("m3").alias("sj_unit_of_measure"),
            commodity.alias("commodity_type"),
            allocation.alias("allocation_method"),
            effective_from.alias("effective_from_date"),
            range_days.alias("range_days"),
            rec_ts.alias("__record_timestamp"),
        )
        .withColumn("effective_to_date", F.expr("date_add(effective_from_date, range_days)"))
        .drop("range_days")
    )

    table = f"{catalog}.{schema}.{POD_TABLE}"
    pod.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    _enable_cdf(spark, table)
    print(f"  ✓ {table}: {n_rows:,} rows across ~{n_distinct:,} distinct PODs [CDF on]")


# --------------------------------------------------------------------------- #
# Projection (analytic — no data movement)
# --------------------------------------------------------------------------- #

def print_projection(scale: float):
    n_rows = int(FULL_SCALE_POD_ROWS * scale)
    keep = COMMODITY_WEIGHTS["gas"] * ALLOCATION_WEIGHTS["PRF"]   # gas + PRF filter
    eff_rows = n_rows * keep
    mean_days = min(mean_range_days(), MAX_EXPLODE_YEARS * 365)
    daily = eff_rows * mean_days
    hourly = daily * 24
    print("\nProjected downstream scale (analytic estimate):")
    print(f"  POD rows                : {n_rows:,}  (~{eff_rows/1e6:,.2f}M pass gas+PRF filter = {keep:.0%})")
    print(f"  mean range length       : ~{mean_days:,.0f} days")
    print(f"  after daily explode      : ~{daily/1e9:,.2f}B rows  (filtered POD x mean_days)")
    print(f"  after hourly join        : ~{hourly/1e9:,.2f}B rows  (daily x 24)")
    print("  ^ tune RANGE_BUCKETS / --scale to hit your target (full case: ~2.8B daily, ~40B join).")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser(description="Generate ENGIE NL gas range-join repro data.")
    parser.add_argument("--catalog", default=os.environ.get("DATABRICKS_CATALOG"),
                        help="Unity Catalog to write to (REQUIRED — no default).")
    parser.add_argument("--schema", default=os.environ.get("DATABRICKS_SCHEMA", "engie_nl_data"),
                        help="Schema to write to (default: engie_nl_data).")
    parser.add_argument("--scale", type=float, default=float(os.environ.get("SCALE", "0.01")),
                        help="Fraction of full scale for the POD table (default: 0.01 = 1%%).")
    parser.add_argument("--profiles-only", action="store_true", help="Generate only the profile table.")
    parser.add_argument("--pod-only", action="store_true", help="Generate only the POD table.")
    args = parser.parse_args()

    if not args.catalog:
        print("ERROR: no catalog specified. Pass --catalog <name> or set DATABRICKS_CATALOG.\n"
              "       There is intentionally no default catalog.", file=sys.stderr)
        return 2
    if not (0 < args.scale <= 1):
        print("ERROR: --scale must be in (0, 1].", file=sys.stderr)
        return 2

    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

    print(f"📍 Output: {args.catalog}.{args.schema}   (scale={args.scale:g})")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    if not args.pod_only:
        print("Generating gas_profile_fraction (small dimension, full size)...")
        generate_profiles(spark, args.catalog, args.schema)
    if not args.profiles_only:
        print("Generating point_of_delivery...")
        generate_pod(spark, args.catalog, args.schema, args.scale)

    print_projection(args.scale)
    print("\nDone. The pipeline builds the exploded daily table + the final POD x hour table from these.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
