# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

**Scaffolding stage.** Project layout and architecture decisions are locked in (see `architecture.md`), but most pipeline logic is stubbed out or unimplemented:
- `ingestion/manifest.py` — docstring only, `has_been_loaded`/`mark_loaded` not written
- `dags/taxi_transform_and_test_dag.py` — empty stub
- `scripts/backfill.sh` — empty stub
- `tests/test_idempotency.py` — empty stub
- dbt models under `dbt/nyc_taxi/models/` (staging, marts, presentation) and `snapshots/dim_zones_snapshot.sql` exist as files but are not built out
- DAG-to-DAG trigger (DAG 1 → DAG 2) is not wired up: architecture.md specifies asset-based scheduling (`outlets=[Asset(...)]` / `schedule=[asset]`), but the DAG only has a commented-out `TriggerDagRunOperator` sketch

Don't assume a file's existence means its logic is implemented — check contents before building on top of it.

## Commands

Package management is `uv` (see `uv.lock`, `pyproject.toml`, `requires-python = ">=3.12"`).

```bash
uv sync                                    # install deps
uv run pytest                              # run all tests
uv run pytest tests/test_idempotency.py    # run one test file
uv run pytest tests/test_idempotency.py::test_name -v   # run a single test
uv run pytest --cov                        # with coverage (pytest-cov installed)
uv run ruff check .                        # lint
uv run ruff format .                       # format
```

Local stack (Airflow 3.x + two Postgres instances + Redis, via `compose.yaml`):

```bash
docker compose build                       # rebuild custom Airflow image after Dockerfile changes
docker compose up airflow-init             # one-time DB migrate + admin user
docker compose up -d                       # bring up scheduler, api-server, dag-processor, worker, triggerer
docker compose run --rm airflow-cli <args> # ad-hoc `airflow` CLI commands (profiles: debug)
docker compose --profile flower up         # optional Celery monitoring UI (port 5555)
```

Airflow UI: `localhost:8080`. Warehouse Postgres is published to the host at `localhost:5432` (`${POSTGRES_PORT:-5432}`) so host-native dbt can connect directly; the Airflow metadata DB is a separate, unpublished Postgres instance/database (`airflow-metadata-db`) — never treat it as a schema under the warehouse.

dbt (run from `dbt/nyc_taxi/`, against the host-published warehouse Postgres):

```bash
cp profiles.yml.example ~/.dbt/profiles.yml   # first-time setup; not committed, reads env vars
uv run dbt run
uv run dbt test
uv run dbt run --select stg_trips             # single model
uv run dbt seed                               # load taxi_zone_lookup.csv
uv run dbt snapshot                           # dim_zones SCD2
```

Required env vars (see `.env`, not committed): `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.

## Architecture

Full rationale lives in `architecture.md` — read it for *why*, not just *what*. The load-bearing decisions:

**Strict stage separation: Python moves data, dbt transforms it.** dbt never moves data between systems; it only issues SQL against data Python has already landed.

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
| S3 → `raw` | `ingestion/` | bulk load, columns as-is (snake_cased, `source_month` appended) | type coercion beyond Postgres defaults; dropping rows |
| `raw` → `staging` | dbt | cast types, rename, flag `is_valid_trip` | aggregation, cross-entity joins |
| `staging` → `marts` | dbt | build fact/dims | cleaning (already done upstream) |

**Postgres schema layers: `raw` → `staging` → `marts`.** `raw` is not a dbt layer — it's the Python-owned landing zone that dbt reads as a declared `source`. There is deliberately no `intermediate` layer.

**Two DAGs, not one** (`dags/taxi_monthly_ingest_dag.py`, `dags/taxi_transform_and_test_dag.py`): ingest is monthly, dbt iterates many times a day during model development — coupling them means a failing dbt test forces a re-download from TLC. `taxi_monthly_ingest` is also the backfill target: `scripts/backfill.sh` triggers *this DAG* once per month over a range; Airflow `catchup` stays off.

**Idempotency is two-layered, both keyed on `source_month` ("YYYY-MM")**:
- `raw.trips`: `load_month()` does `DELETE WHERE source_month = %s` + `COPY` in one transaction (`ingestion/load_raw.py`) — a rerun never duplicates rows.
- `control.manifest`: audit/idempotency table (`sql/control/manifest.sql`), PK `(taxi_type, source_month)`, upserted (not inserted) by `mark_loaded()` so a rerun updates the existing row. Row presence = loaded, no status column.

Out-of-order backfills (loading month N+2 before N) must not corrupt state — this is meant to be tested explicitly in `tests/test_idempotency.py`, not just asserted.

**`fact_trips` is insert-only by pickup month, not merge/upsert** — TLC doesn't correct published files in place, so upsert complexity was deliberately rejected.

**`dim_zones` is a dbt snapshot (SCD Type 2), not a hand-rolled mart model** — dbt's snapshot feature directly captures `valid_from`/`valid_to`/`is_current`.

**Bad data is flagged, not deleted.** DQ rules (negative fares, zero-distance nonzero-fare, dropoff ≤ pickup, duration > 24h) set `is_valid_trip` in staging; downstream marts filter on the flag rather than the row being dropped at ingest, so exclusions stay auditable.

**Known schema drift to preserve, not paper over:** `cbd_congestion_fee` is absent (not merely null) from TLC's schema before `source_month 2025-01` (NYC congestion pricing, effective 2025-01-05). `raw.trips` already models this as a nullable column; staging/marts logic should treat the absence as a documented drift event, not a data quality defect.

**`ingestion/tlc.py` owns TLC URL and S3 key addressing** — both `extract.py` and `load_raw.py` depend on it; it's the seam that keeps ingestion testable without Airflow.

## Environment split to keep in mind

`ingestion/` code runs on the host at Python 3.12 (dbt, ad-hoc runs) *and* inside the Airflow worker container at Python 3.14 (custom image, see `Dockerfile`) — it needs to work on both. `requires-python` in `pyproject.toml` should stay wide enough to cover this rather than being narrowed to match one environment.
