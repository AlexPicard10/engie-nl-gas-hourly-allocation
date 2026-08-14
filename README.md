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
   version `V` (only PODs changed since the backfill — never the 12M history), and for
   the changed EANs re-expands their current record to hourly grain and runs a scoped
   **retracting MERGE** into the same physical table via a Delta sink: it inserts new
   hours, updates changed ones, and **deletes hours that are no longer valid** (a shrunk
   range's dropped tail, or a deleted POD). Just two pipeline objects — a change-feed
   append flow and a `foreach_batch_sink`. Run on a daily schedule; never full-refresh.

## Prerequisites

**Source tables** (in your own catalog/schema):

- `point_of_delivery` — Delta table. Columns used: `point_of_delivery_ean`,
  `profile_category_code`, `sja`, `sj_unit_of_measure`, `commodity_type`,
  `allocation_method`, `effective_from_date`, `effective_to_date`, `__record_timestamp`.
- `gas_profile_fraction` — Delta table. Columns used: `profile_category_code`,
  `supply_start_date_time_utc`, `profile_fraction`, `supply_hour`, `supply_date`.

**Change Data Feed MUST be enabled on `point_of_delivery`** — the incremental pipeline
reads its change feed, and CDF only records changes committed *after* it is turned on, so
enable it **before** the backfill so nothing is missed at the seam:

```sql
ALTER TABLE <your_catalog>.<source_schema>.point_of_delivery
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

(If you use the generator below, CDF is enabled automatically.)

**Compute / tooling:**
- `init_backfill.py` runs on **Databricks Connect serverless** — Python 3.12 +
  `databricks-connect>=16.4` locally, authenticated to the workspace.
- The incremental pipeline runs as a **serverless SDP (Lakeflow Declarative) pipeline**,
  triggered (scheduled daily via a Workflow).
- The target hourly table is created/owned by `init_backfill.py`; the pipeline appends to
  it via a Delta sink, so no separate table setup is needed.

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
(Change Data Feed must be enabled on the source — see Prerequisites.)

## Why the increment is Python, not SQL

The team's standard flows are SQL (`AUTO CDC INTO`). This one can't be, for one reason:
**retraction**. When a POD's range shrinks (e.g. `valid_to` 9999 → 2026-09-30) or a POD
is deleted, the now-invalid hourly rows must be **deleted**. SQL `AUTO CDC INTO` only does
keyed upserts — the orphaned tail hours are simply absent from the new data, so an upsert
never touches them and they linger. Deleting them needs a `MERGE ... WHEN NOT MATCHED BY
SOURCE ... DELETE`, and a MERGE inside a stream requires `foreach_batch`, which is
Python-only. (Two lesser reasons: writing into a table the pipeline doesn't own also needs
a Python Delta sink; and the change-feed bootstrap read is cleanest in the Python API.)

**So the real question is functional, not technical:** *must invalid hours be retracted
incrementally?* If corrections are rare and a periodic partition rebuild is acceptable,
the increment collapses to a standard SQL `AUTO CDC` flow and the Python exception
disappears. Decide this with the data owner. If Python stays, it is deliberately small —
two objects, one MERGE — so it can be wrapped as a reusable parameterised component rather
than hand-written per pipeline.

## Reload / new-environment runbook (start version)

`engie.cdf_starting_version` is used **only on the pipeline's first run** to know where to
begin reading the change feed; after that the pipeline's own checkpoint drives it and the
version is never used again. Set it to **`V`** (the value `init_backfill.py` prints) — not
`V+1`: `readChangeFeed` **fails at startup** if `startingVersion` is beyond the table's
latest commit, and right after a backfill `V+1` doesn't exist yet. Starting at `V` (which
always exists) is safe; commit `V` is re-read once but the keyed MERGE makes that
idempotent (no duplicates).

Because `V` differs per environment and an old version can eventually be vacuumed, treat a
**full history reload** as a coordinated, infrequent operation:

1. Re-run `init_backfill.py` (rebuilds history; prints the new `V`).
2. Set `engie.cdf_starting_version = V` (the freshly printed value) on the pipeline.
3. **Full-refresh** the pipeline once (resets the checkpoint so it re-bootstraps from the
   new `V`). Note: full-refresh resets the checkpoint but does **not** clean the target
   table — the backfill already wrote it, and the MERGE reconciles.

Normal daily runs need none of this — only a reload does.

## Validated

**Faithful logic test (26/26 checks):** backfill, range-shrink (tail deleted), hard delete
(all rows removed), re-open to open-ended, backdated front-shrink, superseding correction
(latest `__record_timestamp` wins), gas→electricity migration (old gas hours retracted),
multi-EAN batch, and no-op — all with zero duplicate keys.

**Live SDP pipeline run (this exact code):** bootstrap-at-`V` starts cleanly; a run
carrying a shrink + a hard delete produced the correct retraction (shrunk POD's tail gone,
deleted POD fully removed, untouched POD intact, 0 duplicates); a subsequent no-change run
read only from the checkpoint, didn't crash, and left the table byte-identical.

**Performance (synthetic, full scale):** history ~7B rows in ~8 min (3 years), 0 duplicate
keys, offtake reconciles to `sja`; a 1-POD increment ~53 s vs ~62 min reading the full
source naively. **Measure MERGE performance on the real data and volume** — the right data
layout to keep retraction fast needs tuning against the actual table, not this synthetic set.

## Open items

- **Performance of the retracting MERGE at real volume** — needs measurement in ENGIE's
  environment; the target's partitioning/layout may want tuning once change patterns are known.
- **Highest-value question:** do consumers need physical hourly rows, or would aggregates /
  query-time expansion suffice? If so, most of the cost — and the retraction complexity —
  disappears.
