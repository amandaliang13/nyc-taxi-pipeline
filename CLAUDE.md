# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working conventions

Never commit or push without explicit approval, even when the work feels done.

## Project status

**Ingestion built; transform not yet.** The project layout and architecture decisions are final (see `architecture.md`). The ingestion layer (`ingestion/`, DAG 1 tasks, `sql/` DDL) is implemented and covered by `tests/test_idempotency.py` (rerun, out-of-order backfill, rollback, schema drift — ingestion layer only; the dbt/`fact_trips` half comes with the marts). Still stubbed out or unimplemented:
- `dags/taxi_transform_and_test_dag.py` — empty stub
- `scripts/backfill.sh` — empty stub
- dbt models under `dbt/nyc_taxi/models/` (staging, marts, presentation) and `snapshots/dim_zones_snapshot.sql` exist as files but are not built out
- The DAG-to-DAG trigger (DAG 1 → DAG 2) is not wired up yet. `architecture.md` specifies asset-based scheduling (`outlets=[Asset(...)]` / `schedule=[asset]`). `taxi_monthly_ingest_dag.py` only has a bare `loaded` reference and a `# TODO: Use asset-based DAGs to trigger dbt DAG` comment. No trigger mechanism exists yet.

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

dbt (run from `dbt/nyc_taxi/`, against the host-published warehouse Postgres):

```bash
cp profiles.yml.example ~/.dbt/profiles.yml   # first-time setup; not committed, reads env vars
uv run dbt run
uv run dbt test
uv run dbt run --select stg_trips             # single model
uv run dbt seed                               # load taxi_zone_lookup.csv
uv run dbt snapshot                           # dim_zones SCD2
```

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
| S3 → `raw` | `ingestion/` | bulk load, columns as-is (snake_cased, `source_month` appended) | type coercion beyond Postgres defaults, or dropping rows |
| `raw` → `staging` | dbt | cast types, rename, flag `is_valid_trip` | aggregation, cross-entity joins |
| `staging` → `marts` | dbt | build fact/dims | cleaning (already done upstream) |

**Postgres schema layers: `raw` → `staging` → `marts`.** `raw` is not a dbt layer — it's the Python-owned landing zone that dbt reads as a declared `source`. There is deliberately no `intermediate` layer.

**Two DAGs, not one** (`dags/taxi_monthly_ingest_dag.py`, `dags/taxi_transform_and_test_dag.py`). Ingest runs monthly. dbt iterates many times a day during model development. Coupling the two DAGs means a failing dbt test forces a re-download from TLC. `taxi_monthly_ingest` is also the backfill target. `scripts/backfill.sh` triggers *this DAG* once per month over a range. Airflow `catchup` stays off.

**Trigger mechanism: asset-based scheduling, not `TriggerDagRunOperator`.** DAG 1 declares `outlets=[Asset("raw_trips_loaded")]`; DAG 2 sets `schedule=[asset]`. The asset gives an explicit, UI-visible data-lineage edge between the two DAGs instead of an imperative trigger call — not yet wired up (see Project status above).

**Idempotency is two-layered, both keyed on `source_month` ("YYYY-MM")**:
- `raw.trips`: `load_month()` does `DELETE WHERE source_month = %s` + `COPY` in one transaction (`ingestion/load_raw.py`) — a rerun never duplicates rows.
- `control.manifest`: audit/idempotency table (`sql/control/manifest.sql`), PK `(taxi_type, source_month)`, upserted (not inserted) by `mark_loaded()` so a rerun updates the existing row. Row presence = loaded, no status column.

Out-of-order backfills (loading month N+2 before N) must not corrupt state — this is meant to be tested explicitly in `tests/test_idempotency.py`, not just asserted.

**`fact_trips` is insert-only by pickup month, not merge/upsert** — TLC doesn't correct published files in place, so upsert complexity was deliberately rejected.

**`dim_zones` is a dbt snapshot (SCD Type 2), not a hand-rolled mart model** — dbt's snapshot feature directly captures `valid_from`/`valid_to`/`is_current`.

**Bad data is flagged, not deleted.** DQ rules (negative fares, zero-distance nonzero-fare, dropoff ≤ pickup, duration > 24h) set `is_valid_trip` in staging. Downstream marts filter on the flag instead of dropping the row at ingest, so exclusions stay auditable.

**Known schema drift to preserve, not paper over:** `cbd_congestion_fee` is absent (not merely null) from TLC's schema before `source_month 2025-01` (NYC congestion pricing, effective 2025-01-05). `raw.trips` already models this as a nullable column. Staging and marts logic should treat the absence as a documented drift event, not a data quality defect.

**`ingestion/tlc.py` owns TLC URL and S3 key addressing.** Both `extract.py` and `load_raw.py` depend on it. It's the seam that keeps ingestion testable without Airflow.

## Environment split to keep in mind

`ingestion/` code runs on the host at Python 3.12 (dbt, ad-hoc runs). It also runs inside the Airflow worker container at Python 3.14 (custom image, see `Dockerfile`). It needs to work on both. Keep `requires-python` in `pyproject.toml` wide enough to cover this. Don't narrow it to match only one environment.
