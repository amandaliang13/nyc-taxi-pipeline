# NYC Taxi Pipeline

End-to-end batch ELT pipeline for NYC TLC yellow taxi trip records (2023–2025).
Built to demonstrate production-realistic data engineering judgment: idempotent
ingestion, authentic schema drift handling, SCD Type 2, and explicit data quality
contracts.

See `architecture.md` for full design decisions and rationale.

ingestion/ (Python) = Extract + Load, "dumb" movement. Pull TLC Parquet, land it in S3 raw/ unmodified, then bulk-load it into a Postgres raw/landing schema — minimal type coercion, no business logic, no filtering.
dbt/ (SQL) = Transform + Test, "smart" logic. All actual cleaning — type casting, is_valid_trip flagging, the DQ thresholds — belongs in stg_trips.sql, not Python.

## Status
Ingestion layer built (TLC → S3 → Postgres `raw`, with a `control.manifest` audit table) and covered by idempotency tests, plus the backfill script. dbt staging and marts are built: `fact_trips` (incremental by `source_month`), `dim_zones` (SCD Type 2), `dim_date`, `dim_payment_type`, `dim_vendor`, `dim_rate_code`. The presentation mart and DAG 2 are not implemented yet.

## Data model notes

**`dim_zones`: what is Type 1 and what is Type 2.** TLC publishes its zone lookup as a single undated file that is overwritten in place. Known revisions only touched placeholder rows 264/265 and added `service_zone`, with no effective dates, so the lookup has no history to track. The Type 2 attribute is **Congestion Relief Zone (CRZ) membership** (`is_in_congestion_relief_zone`), which changed on 2025-01-05 when MTA tolling started. Zone name, borough and service_zone are **Type 1**: every version shows today's value. `fact_trips` stores the pickup/dropoff zone version keys, resolved by pickup time, so a 2024 trip in Midtown points at the pre-CRZ version and a 2025 trip at the CRZ one.

**CRZ zone list provenance.** `dbt/nyc_taxi/seeds/congestion_relief_zone.csv` is the 38 taxi zones in the MTA dataset "MTA Central Business District Taxi Zones" (Metropolitan Transportation Authority, [data.ny.gov `yfdc-w5jh`](https://data.ny.gov/Transportation/MTA-Central-Business-District-Taxi-Zones/yfdc-w5jh/about_data); issued 2024-08-16, modified 2024-09-04; downloaded 2026-11-04). The `Polygon` column was dropped. **`effective_from` (2025-01-05) is not in the source; it was added here** as the tolling start date.

Cross-check against the data (2026-05, valid trips that start and end in the same zone, zones with ≥ 100 such trips): the share charged `cbd_congestion_fee` is 68–98% for the 35 MTA zones and averages 3% elsewhere. The outliers are the zones straddling 60th St that MTA left out, where a same-zone trip can still enter the CRZ: Lincoln Square East (44%), Central Park (33%), Upper East Side South (26%), Lincoln Square West (21%), Lenox Hill West (19%), Lenox Hill East (16%).

**`taxi_zone_lookup_snapshot`** records changes to the *committed* zone lookup seed. It only captures a TLC revision if someone re-downloads the file and commits it. Nothing joins to it.

## Runbook

**When `dbt/nyc_taxi/seeds/congestion_relief_zone.csv` changes** (a new CRZ event, or an existing `effective_from` edited), the zone version keys in `fact_trips` go stale, and nothing in the manifest tells dbt to rebuild. Run:

```bash
uv run --env-file .env dbt seed -s congestion_relief_zone --project-dir dbt/nyc_taxi
uv run --env-file .env dbt run -s dim_zones --project-dir dbt/nyc_taxi
# then either rebuild everything:
uv run --env-file .env dbt run -s fact_trips --full-refresh --project-dir dbt/nyc_taxi
# or at minimum rebuild from the month before the earliest affected date:
uv run --env-file .env dbt run -s fact_trips --vars '{fact_rebuild_from: "YYYY-MM"}' --project-dir dbt/nyc_taxi
```

- The **earliest affected date** is the new event's `effective_from`, or `min(old, new)` when an existing `effective_from` is changed.
- **Why the month before:** a trip from that date can sit in the previous month's file. Zone version keys are only resolved for pickups within a day of their file's month (`is_pickup_outside_source_month` rows get NULL keys), so the previous month is as far back as an affected keyed trip can be, and the rule is exact. Stray pickups that are further off, even years, carry no key and can't go stale. Example: `effective_from` 2025-01-05 → `fact_rebuild_from: "2024-12"`. Applied uniformly, the rule sometimes rebuilds one extra month, but it avoids date-arithmetic edge cases.
- `dbt test -s assert_trip_zone_version_covers_pickup` fails if this step is skipped or started too late. It names the failing trips and the reason (`key not in dim_zones` / `version dates do not cover pickup`).
- For wide rebuilds, `--full-refresh` is faster: rebuilding both loaded months via delete+insert took ~7 min, a full refresh ~1.5 min.

**When TLC republishes a month**, re-run the backfill for that month, e.g. `scripts/backfill.sh 2024-03 2024-03`. DAG 1 doesn't skip loaded months, so `load_raw` replaces the month, `mark_loaded` moves `control.manifest.loaded_at`, and the next dbt run replaces that month (and only that month) in `fact_trips`. `trip_key`s of a republished month may change, since TLC can reorder rows.

**Existing warehouses and `raw.trips.source_row_number`.** `sql/raw/trips.sql` adds the column with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, nullable, because older rows have no file position. Either reload each month (`scripts/backfill.sh`), or derive it from `id`, which `load_month` assigns in file order. Only do the latter after checking that each month's ids are contiguous (one load per month: `max(id) - min(id) + 1 = count(*)`):

```sql
UPDATE raw.trips t
SET source_row_number = t.id - m.min_id + 1
FROM (SELECT source_month, min(id) AS min_id FROM raw.trips GROUP BY 1) m
WHERE m.source_month = t.source_month;
ALTER TABLE raw.trips ALTER COLUMN source_row_number SET NOT NULL;
```

## Setup
TODO: fill in once compose.yaml and the dbt profile are fully wired up.

## Tests
```bash
docker compose up -d warehouse-db
uv run pytest -v -rs
```
`tests/test_idempotency.py` checks rerun safety, out-of-order backfills, mid-load rollback, stable `source_row_number`s and the `cbd_congestion_fee` / `request_source` schema drift against a throwaway Postgres database and mocked S3. Without a reachable Postgres the tests skip rather than fail.
