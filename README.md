# ENGIE NL — Gas Hourly Consumption Allocation

Working solution for the range-join / fan-out performance problem: two small source
tables (~12M + ~316k rows) expanded into a ~40–60B-row hourly allocation table.
**Split history from increment** — batch-load the history once, then process only
daily changes via the source Change Data Feed. One physical table, incremental
refresh, full POD×hour grain.

Detailed write-up (shareable): see the accompanying Google Doc.

## Files

| File | Role |
|------|------|
| `init_backfill.py` | One-time history load (standalone Spark batch, per year). |
| `pipeline_incremental/engie_gas_incremental.py` | Daily incremental SDP pipeline (reads source Change Data Feed → Delta sink into the same table). |
| `generate_engie_nl_data.py` | Synthetic source-data generator (tunable scale), for reproduction/testing. |

## How it works

1. **History (batch, once):** `init_backfill.py` explodes POD ranges to daily grain,
   dedups (latest `__record_timestamp` wins) with a window function, broadcast-joins
   the small profile table, and appends per-year into a table partitioned by
   `(supply_year, supply_month)` — no clustering on write. It prints the pinned
   source Delta version `V`.
2. **Increment (SDP pipeline, daily):** reads the source **Change Data Feed** from
   version `V` (only PODs changed since the backfill — never the 12M history),
   dedups the delta via AUTO CDC SCD-1, fans out to hourly, and appends into the same
   physical table via a Delta sink. Run on a daily schedule; never full-refresh.

## (Optional) Generate synthetic source data

You already have the real source tables, so this step is **optional** — but it's handy
for reproducing the behaviour, benchmarking at a chosen scale, or testing the pipeline in
a scratch schema. `generate_engie_nl_data.py` creates the two source tables
(`point_of_delivery` + `gas_profile_fraction`) with realistic distributions and Change
Data Feed already enabled.

```bash
# needs Python 3.12 + databricks-connect>=16.4 (serverless)
# --scale is a fraction of full size: 1.0 = ~12M PODs (full), 0.05 = ~600k (fast test)
python generate_engie_nl_data.py \
  --catalog <your_catalog> --schema <source_schema> \
  --scale 1.0
```

Options: `--scale` (0<scale≤1, default 0.01), `--pod-only`, `--profiles-only`
(regenerate just one table). `gas_profile_fraction` is always built at full size (it's
small); `--scale` only affects the `point_of_delivery` row count. The generator also
prints the projected downstream fan-out for the chosen scale.

## Run

```bash
# 1. history (prints pinned version V)
python init_backfill.py \
  --catalog <cat> --schema <out_schema> \
  --source-schema <cat>.<src_schema> \
  --years 2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025

# 2. deploy the incremental pipeline with engie.cdf_starting_version = V, serverless,
#    triggered; schedule it daily via a Workflow.
```

Config keys for the pipeline: `engie.source_schema`, `engie.target_table`,
`engie.cdf_starting_version` (= V), `engie.max_explode_years` (default 10).
Requires `delta.enableChangeDataFeed = true` on the source `point_of_delivery`.

## Measured (full data scale)

- History: ~7B rows in ~8 min (3 years), 0 duplicate keys, offtake reconciles to sja.
- Daily increment (1 new POD): **~53 s**, history untouched — vs ~62 min reading the
  full source naively.

## Open items

- **Deletes / historical corrections** to already-materialized rows need a targeted
  per-partition rebuild (append can't retract).
- **Highest-value question:** do consumers need physical hourly rows, or would
  aggregates / query-time expansion suffice? If so, most of the cost disappears.
