-- control.manifest: idempotency/audit log of loaded (taxi_type, source_month)
-- partitions in raw.trips. Written by ingestion/manifest.py's mark_loaded(), in the same
-- transaction as the raw.trips DELETE+COPY (see load_raw_postgres in DAG 1).
-- mark_loaded() upserts on conflict rather than inserting, so a rerun updates the
-- existing row instead of duplicating -- mirrors raw.trips' own DELETE+COPY idempotency.
-- No status column: row presence = loaded, full stop.

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.manifest (
    taxi_type    text NOT NULL DEFAULT 'yellow',
    source_month text NOT NULL,
    s3_key       text NOT NULL,
    row_count    integer NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (taxi_type, source_month),
    CONSTRAINT manifest_source_month_format CHECK (source_month ~ '^\d{4}-(0[1-9]|1[0-2])$')
);

-- statement_timestamp(), not now(): the upsert shares a transaction with the raw.trips
-- DELETE+COPY, and now() returns the time that transaction started, minutes earlier.
-- CREATE TABLE IF NOT EXISTS won't change the default on an existing warehouse.
ALTER TABLE control.manifest ALTER COLUMN loaded_at SET DEFAULT statement_timestamp();

COMMENT ON TABLE control.manifest IS
    'Idempotency/audit log of loaded (taxi_type, source_month) partitions in raw.trips. Row presence = loaded; mark_loaded() upserts on rerun.';

COMMENT ON COLUMN control.manifest.taxi_type IS
    'TLC taxi type loaded (currently always "yellow").';
COMMENT ON COLUMN control.manifest.source_month IS
    'Partition tag "YYYY-MM", matches raw.trips.source_month.';
COMMENT ON COLUMN control.manifest.s3_key IS
    'S3 key of the parquet object loaded for this partition.';
COMMENT ON COLUMN control.manifest.row_count IS
    'Rows loaded into raw.trips for this partition.';
COMMENT ON COLUMN control.manifest.loaded_at IS
    'Most recent load time: when its manifest upsert ran (statement_timestamp()), just before the load committed. Updated on rerun, not appended.';
