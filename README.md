# NYC Taxi Pipeline

Batch ELT pipeline for NYC TLC yellow taxi trips (2024–2026). It shows idempotent ingestion, schema drift handling, SCD Type 2 and explicit data quality rules.

- **Python (`ingestion/`)** moves data: TLC Parquet → S3 → Postgres `raw`, unmodified.
- **dbt (`dbt/nyc_taxi/`)** transforms it: `raw` → `staging` → `marts` (star schema).

Design decisions and rationale: [`architecture.md`](architecture.md).

## Status

Built: ingestion, the backfill script, dbt staging and dbt marts. Not built yet: the presentation mart and DAG 2.

## Quick start

```bash
cp .env.example .env                       # then fill in the values
docker compose up airflow-init             # once
docker compose up -d                       # Airflow UI: localhost:8080
scripts/backfill.sh 2024-01 2024-03        # load months

uv run dbt deps   # dbt reads .env itself: DBT_PROJECT_DIR / DBT_PROFILES_DIR point at dbt/nyc_taxi
uv run dbt build --exclude mart_zone_hourly_demand
```

## Tests

```bash
docker compose up -d warehouse-db
uv run pytest -rs
```

The tests use a throwaway Postgres database and mocked S3. If Postgres isn't reachable, they skip instead of failing. Check the `-rs` output.

## Runbook

### The CRZ seed changes

`dbt/nyc_taxi/seeds/congestion_relief_zone.csv` changed (a new event, or an edited `effective_from`). The zone keys in `fact_trips` are now stale.

```bash
uv run dbt seed -s congestion_relief_zone
uv run dbt run -s dim_zones
uv run dbt run -s fact_trips --full-refresh
```

To rebuild less, replace `--full-refresh` with `--vars '{fact_rebuild_from: "YYYY-MM"}'`. Use the month **before** the earliest affected date. For a new event, that date is its `effective_from`. For an edited one, it is the earlier of the old and new dates. Example: `2025-01-05` → `"2024-12"`.

`dbt test -s assert_trip_zone_version_covers_pickup` fails until you rebuild `fact_trips`.

### A DQ rule or threshold changes

A rule in `stg_trips.sql` changed, or `min_flag_drop_fare` changed in `dbt_project.yml`. `fact_trips` is incremental, so the months it already built keep their old flags.

```bash
uv run dbt run -s fact_trips --full-refresh
```

`dbt test -s assert_dq_flags_match_rules` fails until you rebuild `fact_trips`. Change `min_flag_drop_fare` in `dbt_project.yml`, not with `--vars` on one run. Built months keep the value their run used, and the test checks them against the project value.

### TLC republishes a month

```bash
scripts/backfill.sh 2024-03 2024-03
```

The next dbt run replaces that month, and only that month, in `fact_trips`.

### An existing warehouse needs `source_row_number`

Apply `sql/raw/trips.sql`. Then reload every month with `scripts/backfill.sh`, or derive the column from `id`. Use `id` only if each month's ids are contiguous (`max(id) - min(id) + 1 = count(*)`):

```sql
UPDATE raw.trips t
SET source_row_number = t.id - m.min_id + 1
FROM (SELECT source_month, min(id) AS min_id FROM raw.trips GROUP BY 1) m
WHERE m.source_month = t.source_month;
ALTER TABLE raw.trips ALTER COLUMN source_row_number SET NOT NULL;
```

Then run `dbt build --full-refresh`.
