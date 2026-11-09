# NYC Taxi Pipeline — Architecture Decision Doc
 
Status: This doc predates the code and is expected to be revised as the project develops. The core decisions below should not change without a documented reason.
 
## 1. Problem framing
 
This project is an end-to-end batch ELT pipeline for NYC TLC yellow taxi trip records, built to demonstrate production-realistic data engineering judgment.

It differs from the standard "taxi pipeline" portfolio project in four ways:
- authentic multi-year schema drift
- explicit data quality contracts
- idempotent, backfillable ingestion
- SCD Type 2 on a dimension that actually changes
 
## 2. Data scope
 
**Source:** NYC TLC yellow taxi trip records (official TLC source), published monthly as Parquet.
 
**Years included:** 2023, 2024, 2025 (full years, contiguous). ~15–20M trips total.
 
**Why contiguous 2023–2025, not a COVID-excluding split:**
- Delivers genuine, recent schema drift for free. TLC added a `cbd_congestion_fee` column starting with 2025 data, tied to NYC's congestion pricing toll that took effect January 5, 2025.
- Note: TLC publishes with a ~2-month lag, so full-year 2025 data should be complete. Verify that TLC has published the final month or months before you finalize the load.

**Why not smaller / why not larger:**
- Floor: below ~5M rows, Postgres query planner doesn't meaningfully punish a missing index — the EXPLAIN ANALYZE / query optimization work has no teeth.
- Ceiling: full 2009–2024 archive (~2B+ trips) is unnecessary for local Postgres iteration speed and isn't what this project is trying to prove. Partitioning strategy is *designed* as if scaling to full history, but not physically loaded.
- ~15–20M rows fits comfortably in local Postgres (single-digit GB). It keeps dbt and Airflow iteration cycles fast. It is also large enough that a bad query plan becomes visible and demonstrable.

## 3. Data model
 
**Grain:** one row = one trip (yellow taxi pickup-to-dropoff event).
 
**Schema: star schema, Kimball-style.**
 
| Table | Type | Notes |
|---|---|---|
| `fact_trips` | Fact | `trip_key` (from `source_month` + `source_row_number`), pickup/dropoff datetime, location_id FKs plus pickup/dropoff zone version keys (resolved by pickup time; NULL when the pickup is outside the file's month), passenger_count, trip_distance, fare/tip/total amounts, payment_type_id / vendor_id / ratecode_id FKs, every per-rule DQ flag + `is_valid_trip`. Keeps invalid rows |
| `dim_zones` | Dimension, **SCD Type 2** | location_id, borough, zone_name, service_zone, is_in_congestion_relief_zone, valid_from, valid_to, is_current. TLC publishes the zone lookup as a single undated file that is overwritten in place; known revisions only touched placeholder rows 264/265 and added `service_zone`, with no effective dates, so its history can't be reconstructed. The Type 2 attribute is instead Congestion Relief Zone membership: real, officially sourced (MTA's CBD taxi-zone list), and dated (2025-01-05). Zone name, borough and service_zone are Type 1 |
| `dim_payment_type` | Dimension, static | small lookup, cash/card/no-charge/etc |
| `dim_vendor` | Dimension, static | trivial but included for completeness |
| `dim_rate_code` | Dimension, static | TLC RatecodeID values (data dictionary, March 2025) |
| `dim_date` | Dimension, standard | date, day-of-week, month, year, is_holiday — standard practice, cheap to build. Spine 2022-12-31 → 2030 from vars |
 
**Bad-data handling decision:** invalid/anomalous trips (negative fares, zero-distance nonzero-fare, dropoff-before-pickup, out-of-bounds coordinates, duration > 24h) are **flagged, not deleted**, via `is_valid_trip` boolean set in staging. Rationale: deleting at ingest destroys the ability to audit which rows were excluded and why. Downstream marts filter on the flag instead. 

**Data quality thresholds (set in `stg_trips`, one boolean flag per rule):**
- fare_amount < 0 → invalid
- trip_distance = 0 AND fare_amount > minimum flag-drop fare → invalid (dbt var `min_flag_drop_fare`, default 3.00)
- dropoff_datetime <= pickup_datetime → invalid
- trip duration > 24h → invalid
- pickup or dropoff `location_id` not in the zone lookup, or = 264 (the lookup's "Unknown" placeholder) → invalid. Coordinate-based bounding-box checks are N/A for this scope — 2023+ TLC data uses `location_id` (zone-based), not lat/long.
- pickup date outside the file's `source_month`, with a one-day tolerance either side for trips crossing a month boundary → invalid. TLC files carry stray rows, some years off (2001 and 2008 pickups in the 2023-03 and 2026-05 files). This is a DQ problem in its own right; the `dim_date` FK test filters on the flag as a consequence, not the other way round.
- `is_outside_nyc` (zone 265, "Outside of NYC") is informational and not part of `is_valid_trip`: trips to Newark or Westchester are real trips.
## 4. Pipeline architecture
 
**Orchestrator: Airflow 3.x, not 2.x** (decided 2026-09-01). Airflow 2's open-source support ended April 22, 2026. AWS MWAA and GCP Cloud Composer both now run 3.x in production. Building new on 2.x would mean shipping a portfolio project on dead tooling.
```
TLC monthly source (Parquet, official TLC site)
  → Airflow DAG 1: monthly ingest, idempotent (rerun = no duplication)
      — S3 → Postgres `raw` schema
  → Airflow DAG 2: dbt run + dbt test
  → dbt staging models (stg_trips, stg_zones, stg_payment_types) → `staging` schema
      — type casting, is_valid_trip flagging, no aggregation
  → dbt marts models → `marts` schema
      — fact_trips (incremental), dim_zones (SCD2), dim_payment_type, dim_vendor, dim_date
  → dbt tests
      — schema tests (not_null, unique, relationships) + custom singular tests for the DQ rules above
  → presentation-layer aggregate mart
      — mart_zone_hourly_demand or similar, answers a real stakeholder question
```
 
### Responsibility per stage
 
Strict separation: Python moves data, dbt transforms it. dbt never moves data between systems. It only issues SQL against a database that already holds the data. This is why the S3 → Postgres load is Python's job, and dbt picks up from `raw` via a declared **source**.
 
| Stage | Owner | Does | Must NOT do |
|---|---|---|---|
| TLC → S3 | `ingestion/` (Python) | Download monthly Parquet, write to S3 unmodified | Any cleaning, filtering, or reshaping |
| S3 → `raw` schema | `ingestion/` (Python) | Bulk load Parquet into Postgres, columns as-is | Type coercion beyond what Postgres forces, or dropping rows |
| `raw` → `staging` | dbt models | Cast types, rename columns, flag `is_valid_trip` | Aggregation, or joins between entities |
| `staging` → `marts` | dbt models | Build star schema (fact + dimensions) | Cleaning — already done upstream |
| Tests | dbt tests | Assert correctness at each layer | — |
 
Schema routing is dbt *config*, not per-model SQL: `dbt_project.yml` sets a schema per folder (`staging`, `marts`; presentation models also land in `marts`; seeds in `seeds`, snapshots in `snapshots`). `macros/generate_schema_name.sql` uses it verbatim instead of dbt's default `public_staging`.
### Postgres schema layers
 
`raw` → `staging` → `marts`

`raw` is not a dbt layer at all — it's the landing zone Python writes to and dbt reads as a source.
 
Alongside `raw`, the Python-owned `control` schema holds `control.manifest`, the idempotency/audit log of loaded `(taxi_type, source_month)` partitions. Like `raw`, it's not a dbt layer.

Airflow's own metadata DB lives in a **separate Postgres instance** (`airflow-metadata-db` in `compose.yaml`, not published to the host) — not a schema under `raw`/`staging`/`marts`, and not in the warehouse instance.
### Local container build (as built, 2026-09-03)
 
Custom Airflow image rather than stock `apache/airflow:3.3.1` — built from the slim base with only the extras this project actually uses, which roughly halves the image.
 
| Check | Result |
|---|---|
| Image size | 993 MB (vs ~2 GB for stock `apache/airflow:3.3.1`) |
| Python | 3.14.3 |
| Airflow | 3.3.1 |
| `curl` (for healthchecks) | present |
| Providers | postgres 7.0.1, amazon 9.34.0, celery 3.23.1, fab 3.8.0, redis, http |
| flower | 2.0.1 (pulled in by the celery extra — the flower service works) |
 
Files:
 
- **`Dockerfile`** — base `apache/airflow:slim-3.3.1-python3.14`. It installs the `celery,redis,postgres,amazon,fab` extras against `constraints-3.14.txt`.
- **`.dockerignore`** — excludes `.venv/`, `data/`, `.git/`, etc. Without this the build context includes the host venv and any downloaded Parquet, which is most of the size difference.
- **`compose.yaml`** — `x-airflow-common` builds from `./Dockerfile` as `image: nyc-taxi-airflow:3.3.1` rather than pulling the stock image.
- Both Postgres services (Airflow metadata DB, taxi warehouse) on `postgres:18`.
Two version notes worth tracking rather than assuming:
 
- **Host and container both on Python 3.14 (decided 2026-10-30).** `.python-version` pins the host `uv` venv (pytest, dbt, ad-hoc ingestion) to 3.14, matching the worker image, so a host test run exercises the same interpreter the DAGs use. `requires-python = ">=3.12"` stays as a looser floor.
- **Python 3.14 and wheel availability.** Python 3.14 is recent enough that wheel availability for providers and libraries should be verified, not assumed. The constraints file pinned a working set here, so the build is fine today. But a later `uv add` of something unrelated could still hit a package with no 3.14 wheel.
### S3 key convention
 
```
s3://nyc-taxi-datapipeline/raw/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet
```
 
Hive-style `key=value` partitioning, zero-padded month. This was chosen over the plain `/2023/01/` form because Glue crawlers and Athena auto-detect it as partitions.
### DAG split
 
Two DAGs, not one:
 
- **`taxi_monthly_ingest`** — TLC → S3 → `raw`. Monthly, idempotent, matches TLC's publish cadence. This is the backfill target (`scripts/backfill.sh` triggers *this* DAG over a month range, not the whole pipeline).
- **`taxi_transform_and_test`** — `dbt run` + `dbt test`. Triggered on DAG 1 completion, or run standalone during model development.
Rationale for splitting rather than one monolithic DAG:
**(1)Blast radius** a failing dbt test shouldn't force re-downloading from TLC and rewriting S3. **(2)Cadence mismatch** ingest runs monthly.

**Trigger mechanism: Asset-based scheduling.** DAG 1's `load_raw_postgres` task (the one that makes data available to dbt) declares `outlets=[RAW_TRIPS_LOADED]`, i.e. `Asset("raw_trips_loaded")`, defined once in `dags/assets.py`. The event is emitted only when the task succeeds. DAG 2 sets `schedule=[asset]` instead of a cron string or a `TriggerDagRunOperator` call. This was chosen over `TriggerDagRunOperator` because the asset gives an explicit, UI-visible data-lineage edge between the two DAGs, rather than an imperative "and then trigger this" call.

**Ingest granularity: one month per DAG run**, matching TLC's own publish cadence. This keeps the backfill story clean — rerunning N months is a natural, explainable operation, not an artificial batching decision.
 
**Incremental strategy for fact_trips: whole-month delete+insert by `source_month`, not row-level merge/upsert.** TLC does republish files (errata, the 2022 Parquet re-release) and warns of future schema changes. When it corrects data, the unit of change is a whole monthly file, and trip rows have no key that survives a republish, so a row-level merge would have nothing reliable to match on. Replacing the whole month mirrors `load_raw`'s DELETE+COPY.
- A month is rebuilt when `control.manifest.loaded_at` differs from the `loaded_at` it was built from. The test is `is distinct from`, not `>`, so a reload is still picked up if its `loaded_at` isn't later (a clock stepping back, a manifest restored from backup or fixed by hand). A republished month is handled by re-running `scripts/backfill.sh` for it: DAG 1 doesn't skip loaded months, so the month is reloaded, `loaded_at` moves, and dbt rebuilds that month only. `var('fact_rebuild_from')` forces a rebuild for changes the manifest can't see (a CRZ seed change; see the README runbook).
- Keyed on `source_month`, not pickup month: TLC files contain stray pickups from other months.
- No `max(pickup_datetime)` watermark: it would silently skip month N if N+2 was built first.

**Idempotency requirement:** re-running an already-loaded month must not duplicate rows. Out-of-order backfills (loading month N+2 before month N) must not corrupt downstream incremental state. This will be explicitly tested, not just assumed.
 
## 5. Project layout

Status: as built, 2026-10-21. This doc is kept in sync with the repo rather than the original plan — see the deltas noted below.

```
nyc-taxi-pipeline/
├── README.md
├── architecture.md                      # this doc
├── compose.yaml                         # Airflow + Postgres, single command bring-up
├── Dockerfile                           # custom Airflow image, see §4
├── .dockerignore
├── .env                                 # not committed; see .env.example
│
├── dags/
│   ├── assets.py                        # shared Asset definitions (RAW_TRIPS_LOADED: DAG 1 → DAG 2)
│   ├── taxi_monthly_ingest_dag.py       # DAG 1: orchestrates raw ingest, one run per TLC month
│   └── taxi_transform_and_test_dag.py   # DAG 2: dbt run + dbt test — stub, not yet implemented
│
├── ingestion/
│   ├── __init__.py
│   ├── tlc.py                           # addressing: TLC source URLs, S3 key naming, availability check
│   ├── extract.py                       # pulls TLC monthly Parquet, streams straight to S3 raw/ unmodified
│   ├── load_raw.py                      # S3 Parquet → Postgres `raw` schema (bulk load / COPY)
│   └── manifest.py                      # tracks loaded months in control.manifest (upsert)
│
├── sql/                                 # warehouse DDL, mounted into warehouse-db's initdb
│   ├── raw/trips.sql                    # raw.trips landing table
│   └── control/manifest.sql             # control.manifest idempotency/audit table
│
├── dbt/
│   └── nyc_taxi/
│       ├── dbt_project.yml
│       ├── packages.yml                 # dbt_utils
│       ├── profiles.yml.example
│       ├── seeds/
│       │   ├── taxi_zone_lookup.csv     # TLC zone lookup, Type 1 attributes of dim_zones
│       │   ├── congestion_relief_zone.csv  # MTA CBD taxi zones + effective_from, the Type 2 events
│       │   ├── payment_types.csv
│       │   ├── vendors.csv
│       │   ├── rate_codes.csv
│       │   ├── us_holidays.csv          # 2023–2030 federal holidays (scripts/generate_us_holidays.py)
│       │   └── schema.yml               # seed provenance + key tests
│       ├── snapshots/
│       │   └── taxi_zone_lookup_snapshot.sql  # audit trail of the committed lookup seed; nothing joins to it
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_trips.sql        # rename, type, trip_key, per-rule DQ flags + is_valid_trip
│       │   │   ├── stg_zones.sql
│       │   │   ├── stg_payment_types.sql
│       │   │   ├── stg_vendors.sql
│       │   │   ├── stg_rate_codes.sql
│       │   │   └── schema.yml           # source defs (raw, control) + docs
│       │   ├── marts/
│       │   │   ├── fact_trips.sql       # incremental, whole-month delete+insert by source_month
│       │   │   ├── dim_zones.sql        # SCD2 on CRZ membership, from events + lead()
│       │   │   ├── dim_payment_type.sql
│       │   │   ├── dim_vendor.sql
│       │   │   ├── dim_rate_code.sql
│       │   │   ├── dim_date.sql
│       │   │   └── schema.yml
│       │   └── presentation/
│       │       └── mart_zone_hourly_demand.sql
│       ├── tests/
│       │   └── singular/                # zone-version coverage test; custom DQ rule tests come in Phase 6
│       └── macros/
│           └── generate_schema_name.sql # use +schema verbatim (staging, not public_staging)
│
├── tests/
│   ├── conftest.py                      # throwaway test DB, moto S3, synthetic TLC Parquet
│   └── test_idempotency.py              # pytest: rerun + out-of-order backfill don't corrupt state (ingestion layer)
│
└── scripts/
    └── backfill.sh                      # backfill.sh START END (YYYY-MM): one DAG 1 trigger per month
```

Notes on this layout:

- `ingestion/` is plain Python that the Airflow DAG calls. It is kept separate from `dags/` so the extraction logic is independently testable without starting Airflow.
- `dim_zones` is a **mart model built from an event table** (a baseline row per zone, plus one row per CRZ seed entry; `lead()` gives `valid_to`), not a dbt snapshot. A snapshot only records changes it observes between runs and stamps them with the run time, and TLC's lookup has no dated history to observe. Replaying history into a snapshot was considered and rejected: it breaks for good if the first run happens in the wrong order. The event table is deterministic and safe to full-refresh. The snapshot that remains (`taxi_zone_lookup_snapshot`) records changes to the *committed* lookup seed only: it captures a TLC revision only if someone re-downloads and commits the file.
- `tests/test_idempotency.py` is a deliberate, separate test target from dbt tests. dbt tests validate data correctness. This file validates pipeline *behavior* (rerun safety, backfill ordering). That is a different claim, and it should be demonstrated, not just asserted in the README.

**Deltas from the original plan, and why:**
- **`extract.py` absorbed `load_raw_s3.py`.** The original plan split "pull from TLC" and "write to S3" into two files. It was built as one streaming call instead (`requests.get(..., stream=True)` piped straight into `s3.upload_fileobj`). The ~500MB monthly file is never buffered in memory or written to local disk between the two steps. `tlc.py` was pulled out separately to own URL and S3-key addressing. Both `extract.py` and `load_raw.py` depend on it. That is the independent-testability seam now, instead of the extract/S3-write split.
- **`load_raw_postgres.py` → `load_raw.py`.** Same responsibility (S3 Parquet → Postgres `raw`, delete-by-`source_month` + COPY, single transaction), shorter name — there's no `load_raw_s3.py` anymore to disambiguate from.
## 6. Open questions / to revisit
 
- [x] Exact DQ thresholds — set in `stg_trips`, documented in §3.
- [x] Does `dim_zones` SCD2 have enough real historical change events in TLC's lookup table to be a meaningful demo? **No.** The lookup is one undated file overwritten in place (only rows 264/265 and the `service_zone` column ever changed, with no effective dates). SCD2 tracks Congestion Relief Zone membership instead: MTA's official CBD taxi-zone list, effective 2025-01-05. No synthetic augmentation needed.
- [ ] Presentation mart — pick the specific stakeholder question.
- [ ] Verify that TLC populated `cbd_congestion_fee` consistently through all of 2025 (congestion pricing had litigation and delay risk), rather than leaving a mid-year gap. Do this before treating it as a clean schema-drift example.
- [ ] Verify that TLC has published the full set of 2025 monthly files. TLC runs on a ~2-month lag. Do this before finalizing the load.
- [ ] Verify that the pinned `apache-airflow-providers-amazon` and `apache-airflow-providers-postgres` versions are Airflow-3.x-compatible, before the custom operator is written. Most providers caught up long ago. But the version should be pinned and verified, rather than assumed compatible.
