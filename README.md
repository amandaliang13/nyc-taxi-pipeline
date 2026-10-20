# NYC Taxi Pipeline

End-to-end batch ELT pipeline for NYC TLC yellow taxi trip records (2023–2025).
Built to demonstrate production-realistic data engineering judgment: idempotent
ingestion, authentic schema drift handling, SCD Type 2, and explicit data quality
contracts.

See `architecture.md` for full design decisions and rationale.

ingestion/ (Python) = Extract + Load, "dumb" movement. Pull TLC Parquet, land it in S3 raw/ unmodified, then bulk-load it into a Postgres raw/landing schema — minimal type coercion, no business logic, no filtering.
dbt/ (SQL) = Transform + Test, "smart" logic. All actual cleaning — type casting, is_valid_trip flagging, the DQ thresholds — belongs in stg_trips.sql, not Python.

## Status
Ingestion layer built (TLC → S3 → Postgres `raw`, with a `control.manifest` audit table) and covered by idempotency tests. dbt transforms, DAG 2 and backfill script not yet implemented.

## Setup
TODO: fill in once compose.yaml and the dbt profile are fully wired up.

## Tests
```bash
docker compose up -d warehouse-db
uv run pytest -v -rs
```
`tests/test_idempotency.py` checks rerun safety, out-of-order backfills, mid-load rollback and the `cbd_congestion_fee` schema drift against a throwaway Postgres database and mocked S3. Without a reachable Postgres the tests skip rather than fail.
