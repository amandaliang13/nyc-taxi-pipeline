# NYC Taxi Pipeline

Batch ELT pipeline for NYC TLC yellow taxi trips (2023–2025). It shows idempotent ingestion, schema drift handling, SCD Type 2 and explicit data quality rules.

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

mkdir -p ~/.dbt && cp dbt/nyc_taxi/profiles.yml.example ~/.dbt/profiles.yml   # once
uv run --env-file .env dbt deps --project-dir dbt/nyc_taxi
uv run --env-file .env dbt build --project-dir dbt/nyc_taxi --exclude mart_zone_hourly_demand
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
uv run --env-file .env dbt seed -s congestion_relief_zone --project-dir dbt/nyc_taxi
uv run --env-file .env dbt run -s dim_zones --project-dir dbt/nyc_taxi
uv run --env-file .env dbt run -s fact_trips --full-refresh --project-dir dbt/nyc_taxi
```

To rebuild less, replace `--full-refresh` with `--vars '{fact_rebuild_from: "YYYY-MM"}'`. Use the month **before** the earliest affected date. For a new event, that date is its `effective_from`. For an edited one, it is the earlier of the old and new dates. Example: `2025-01-05` → `"2024-12"`.

`dbt test -s assert_trip_zone_version_covers_pickup` fails until you rebuild `fact_trips`.

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
