"""
Validates pipeline *behavior*, not data correctness (that's dbt's job) --
architecture.md §5:
  - re-running an already-loaded month does not duplicate rows
  - out-of-order backfills (month N+2 before month N) do not corrupt
    downstream incremental state

TODO: implement against ingestion.manifest + a test Postgres/S3 fixture.
"""
