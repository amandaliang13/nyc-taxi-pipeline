# NYC Taxi Pipeline

End-to-end batch ELT pipeline for NYC TLC yellow taxi trip records (2023–2025).
Built to demonstrate production-realistic data engineering judgment: idempotent
ingestion, authentic schema drift handling, SCD Type 2, and explicit data quality
contracts.

See `architecture.md` for full design decisions and rationale.

ingestion/ (Python) = Extract + Load, "dumb" movement. Pull TLC Parquet, land it in S3 raw/ unmodified, then bulk-load it into a Postgres raw/landing schema — minimal type coercion, no business logic, no filtering.
dbt/ (SQL) = Transform + Test, "smart" logic. All actual cleaning — type casting, is_valid_trip flagging, the DQ thresholds — belongs in stg_trips.sql, not Python.

## Status
Scaffolding stage — project layout in place, pipeline logic not yet implemented.

## Setup
TODO: fill in once docker-compose.yml and dbt profile are wired up.
