# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working conventions

Never commit or push without explicit approval, even when the work feels done.

## Project status

**Ingestion, dbt staging and marts built; presentation and DAG 2 not yet.** The project layout and architecture decisions are final (see `architecture.md`). Built:
- Ingestion layer (`ingestion/`, DAG 1 tasks, `sql/` DDL), covered by `tests/test_idempotency.py` (rerun, out-of-order backfill, rollback, schema drift, stable `source_row_number` — ingestion layer only; the dbt/`fact_trips` half is Phase 6).
- `scripts/backfill.sh` and DAG 1's side of the trigger: `load_raw_postgres` declares `outlets=[RAW_TRIPS_LOADED]` (`dags/assets.py`).
- dbt foundation: schema routing, seeds, `dbt_utils`, and the staging models (`stg_trips` with per-rule DQ flags + `is_valid_trip`).
- dbt marts: `fact_trips` (incremental by `source_month`), `dim_zones` (SCD2 on CRZ membership), `dim_date`, `dim_payment_type`, `dim_vendor`, `dim_rate_code`; the `taxi_zone_lookup_snapshot` audit trail; the tests the design depends on (full suite is Phase 6).

Still stubbed out or unimplemented:
- `dags/taxi_transform_and_test_dag.py` — empty stub, so nothing consumes the asset yet (`schedule=[RAW_TRIPS_LOADED]` comes with it)
- `models/presentation/mart_zone_hourly_demand.sql` — comment-only; `dbt run` errors on it until it's built, so exclude it (`--exclude mart_zone_hourly_demand`)

A file's existence doesn't mean its logic is written. Check contents before you build on top of it.

## Commands

Package management is `uv` (see `uv.lock`, `pyproject.toml`, `requires-python = ">=3.12"`).

```bash
uv sync                                    # install deps
uv run pytest                              # run all tests
uv run pytest tests/test_idempotency.py    # run one test file
uv run pytest tests/test_idempotency.py::test_name -v   # run a single test
uv run pytest --cov                        # with coverage (pytest-cov installed)
uv run pytest -rs                          # show skip reasons
uv run ruff check .                        # lint
uv run ruff format .                       # format
```

`tests/test_idempotency.py` needs the warehouse Postgres up (`docker compose up -d warehouse-db`). It creates and drops a throwaway `nyc_taxi_test_*` database, so the real warehouse is never touched; S3 is mocked with `moto`. If Postgres isn't reachable the tests **skip**, not fail — check `-rs` before trusting a green run.

Local stack (Airflow 3.x + two Postgres instances + Redis, via `compose.yaml`):

```bash
docker compose build                       # rebuild custom Airflow image after Dockerfile changes
docker compose up airflow-init             # one-time DB migrate + admin user
docker compose up -d                       # bring up scheduler, api-server, dag-processor, worker, triggerer
docker compose run --rm airflow-cli <args> # ad-hoc `airflow` CLI commands (profiles: debug)
docker compose --profile flower up         # optional Celery monitoring UI (port 5555)
```

Airflow UI: `localhost:8080`.

`compose.yaml` publishes warehouse Postgres to the host at `localhost:5432` (`${POSTGRES_PORT:-5432}`), so host-native dbt can connect to it directly. `.env` holds the host-side `POSTGRES_HOST=localhost` / `POSTGRES_PORT`; `compose.yaml` overrides them to `warehouse-db` / `5432` inside the Airflow containers, so the same variable names work on both sides. The Airflow metadata DB lives in a separate, unpublished Postgres instance, as its own database (`airflow-metadata-db`). Never treat it as a schema under the warehouse.

dbt (run from the repo root, against the host-published warehouse Postgres). dbt is a project dependency, so `uv run` installs it into `.venv`. dbt doesn't load `.env` itself; the profile reads it through `--env-file`:

```bash
mkdir -p ~/.dbt && cp dbt/nyc_taxi/profiles.yml.example ~/.dbt/profiles.yml   # once per machine
uv run --env-file .env dbt deps --project-dir dbt/nyc_taxi       # once per checkout: dbt_utils
uv run --env-file .env dbt seed --project-dir dbt/nyc_taxi       # zone lookup, CRZ zones, payment types, vendors, rate codes, holidays
uv run --env-file .env dbt run --project-dir dbt/nyc_taxi        # all models
uv run --env-file .env dbt run --select stg_trips --project-dir dbt/nyc_taxi   # single model
uv run --env-file .env dbt test --project-dir dbt/nyc_taxi
uv run --env-file .env dbt snapshot --project-dir dbt/nyc_taxi   # zone lookup audit trail (nothing joins to it)
uv run --env-file .env dbt run --select fact_trips --vars '{fact_rebuild_from: "YYYY-MM"}' --project-dir dbt/nyc_taxi   # force-rebuild months >= YYYY-MM
uv run python scripts/generate_us_holidays.py                    # regenerate the holiday seed after widening dim_date
```

Build order doesn't matter for `dim_zones` (it's built from events, not the snapshot). When `seeds/congestion_relief_zone.csv` changes, or TLC republishes a month, follow the runbook in `README.md` — `fact_trips` has to be rebuilt.

Required env vars (see `.env`, not committed; see `.env.example` for descriptions): `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET_VAR`.

## Architecture

Full rationale lives in `architecture.md` — read it for *why*, not just *what*. The load-bearing decisions:

**Strict stage separation: Python moves data, dbt transforms it.** dbt never moves data between systems. It only issues SQL against data Python already landed.

```
TLC (Parquet, monthly)
  → S3 raw/ (Hive-partitioned, unmodified)          ingestion/extract.py
  → Postgres `raw` schema (bulk COPY, as-is)         ingestion/load_raw.py
  → dbt staging (type cast, is_valid_trip flag)      dbt/nyc_taxi/models/staging/
  → dbt marts (star schema)                          dbt/nyc_taxi/models/marts/
  → dbt tests + presentation mart
```

| Stage | Owner | Does | Must NOT do |
|---|---|---|---|
| TLC → S3 | `ingestion/` | download, write to S3 unmodified | any cleaning/filtering/reshaping |
| S3 → `raw` | `ingestion/` | bulk load, columns as-is (snake_cased, `source_month` + `source_row_number` appended) | type coercion beyond Postgres defaults, or dropping rows |
| `raw` → `staging` | dbt | cast types, rename, flag `is_valid_trip` | aggregation, cross-entity joins |
| `staging` → `marts` | dbt | build fact/dims | cleaning (already done upstream) |

**Postgres schema layers: `raw` → `staging` → `marts`.** `raw` is not a dbt layer — it's the Python-owned landing zone that dbt reads as a declared `source`. There is deliberately no `intermediate` layer. dbt also writes seeds to `seeds` and snapshots to `snapshots`; presentation models land in `marts`. `macros/generate_schema_name.sql` uses these names verbatim (not dbt's default `public_staging`).

**Two DAGs, not one** (`dags/taxi_monthly_ingest_dag.py`, `dags/taxi_transform_and_test_dag.py`). Ingest runs monthly. dbt iterates many times a day during model development. Coupling the two DAGs means a failing dbt test forces a re-download from TLC. `taxi_monthly_ingest` is also the backfill target. `scripts/backfill.sh` triggers *this DAG* once per month over a range. Airflow `catchup` stays off.

**Trigger mechanism: asset-based scheduling, not `TriggerDagRunOperator`.** DAG 1 declares `outlets=[Asset("raw_trips_loaded")]`; DAG 2 sets `schedule=[asset]`. The asset gives an explicit, UI-visible data-lineage edge between the two DAGs instead of an imperative trigger call. The asset lives in `dags/assets.py`; DAG 1's side is wired, DAG 2's isn't yet (see Project status above).

**Idempotency is two-layered, both keyed on `source_month` ("YYYY-MM")**:
- `raw.trips`: `load_month()` does `DELETE WHERE source_month = %s` + `COPY` in one transaction (`ingestion/load_raw.py`) — a rerun never duplicates rows.
- `control.manifest`: audit/idempotency table (`sql/control/manifest.sql`), PK `(taxi_type, source_month)`, upserted (not inserted) by `mark_loaded()` so a rerun updates the existing row. Row presence = loaded, no status column. `mark_loaded()` runs only after `load_month()` has committed, so a dbt build that sees the new `loaded_at` also sees the new rows.

Out-of-order backfills (loading month N+2 before N) must not corrupt state — this is meant to be tested explicitly in `tests/test_idempotency.py`, not just asserted.

The stable trip identity is `(source_month, source_row_number)` — the row's position in its month's file, hashed into `trip_key`. `raw.trips.id` is regenerated on every reload. A TLC republish may renumber a month, which is fine because every layer replaces a month as a whole.

**`fact_trips` is incremental by `source_month`: whole-month delete+insert, not row-level merge/upsert.** TLC does republish files (errata, the 2022 Parquet re-release), so a month is the unit of change, and trip rows have no key that survives a republish — a row-level merge would have nothing reliable to match on. A month is rebuilt when `control.manifest.loaded_at` differs from (`is distinct from`, not just later than) the one it was built from, so a republished month is handled by re-running `scripts/backfill.sh` for it (DAG 1 doesn't skip loaded months). Not keyed on pickup month, because files carry stray pickups from other months; no `max(pickup_datetime)` watermark, because it would skip month N loaded after N+2.

**`dim_zones` is SCD Type 2 on Congestion Relief Zone membership, built from an event table, not a dbt snapshot.** TLC publishes the zone lookup as a single undated file that is overwritten in place; known revisions only touched placeholder rows 264/265 and added `service_zone`, with no effective dates — so there's no lookup history to capture. CRZ membership (MTA's official CBD taxi-zone list, effective 2025-01-05, `seeds/congestion_relief_zone.csv`) is a real, dated change. Zone name, borough and service_zone are Type 1. `fact_trips` stores the pickup/dropoff zone version keys, resolved by pickup time, and leaves them NULL when `is_pickup_outside_source_month` (a pickup far from its file's month can't be trusted to pick a version). `snapshots/taxi_zone_lookup_snapshot.sql` is only an audit trail of the committed lookup seed; nothing joins to it.

**Bad data is flagged, not deleted.** DQ rules (negative fare, zero distance with fare above `var('min_flag_drop_fare')`, dropoff ≤ pickup, duration > 24h, location_id not in the zone lookup or = 264 "Unknown", pickup date outside the file's month with ±1 day tolerance) each get a boolean flag in `stg_trips`, and `is_valid_trip` = none fired. `is_outside_nyc` (zone 265) is informational, not a rule. Downstream marts filter on the flag instead of dropping the row at ingest, so exclusions stay auditable; `fact_trips` keeps all rows and every flag.

**Known schema drift to preserve, not paper over:** `cbd_congestion_fee` is absent (not merely null) from TLC's schema before `source_month 2025-01` (NYC congestion pricing, effective 2025-01-05). `raw.trips` already models this as a nullable column. Likewise `request_source` (text) is absent before `source_month 2026-06`. Staging and marts logic should treat the absence as a documented drift event, not a data quality defect.

**`ingestion/tlc.py` owns TLC URL and S3 key addressing.** Both `extract.py` and `load_raw.py` depend on it. It's the seam that keeps ingestion testable without Airflow.

## Environment split to keep in mind

Host and container both run Python 3.14. `.python-version` pins the host `uv` venv (pytest, dbt, ad-hoc `ingestion/` runs), and the Airflow worker image is `python3.14` (see `Dockerfile`). Change them together. `requires-python = ">=3.12"` in `pyproject.toml` is only a floor; don't narrow it.
