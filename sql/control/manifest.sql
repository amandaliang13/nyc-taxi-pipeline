-- control.manifest: idempotency/audit log of which (taxi_type, source_month) partitions
-- have been loaded into raw.trips.
--
-- Backs ingestion/manifest.py's has_been_loaded(year, month) / mark_loaded(year, month):
--   - has_been_loaded: SELECT 1 FROM control.manifest WHERE taxi_type = %s AND source_month = %s
--   - mark_loaded: INSERT ... ON CONFLICT (taxi_type, source_month) DO UPDATE, not a
--     bare INSERT -- a rerun of an already-loaded month must update the existing row
--     (new loaded_at, row_count, s3_key) rather than create a duplicate. This mirrors
--     raw.trips' own idempotency mechanism (DELETE+COPY per source_month) at the
--     control-plane level: reloading a month is safe and simply overwrites this table's
--     record of it too.
--
-- Row presence = "this partition has been loaded", full stop -- there is no status/
-- pending state. mark_loaded is only called after raw.trips load succeeds, so a failed
-- or partial load simply leaves no row (or leaves the prior successful row untouched),
-- and has_been_loaded correctly reports false / the last-good state either way.
--
-- Grain is intentionally order-independent: out-of-order backfills (loading
-- source_month N+2 before N) are just two unrelated rows keyed by their own partition,
-- so nothing here needs to enforce or care about load order.
--
-- taxi_type is carried even though the project only loads "yellow" today, because
-- ingestion/tlc.py and extract.py already parameterize every call on taxi_type -- this
-- table keys off the same partition identity those functions use, not a narrower one.
--
-- Consistency between this table and raw.trips (every loaded source_month manifested,
-- and vice versa) is asserted in tests/test_idempotency.py (pytest), not a dbt test --
-- it's a pipeline-behavior claim, not a data-correctness one, and control.manifest is
-- deliberately not exposed to dbt as a source.

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.manifest (
    taxi_type    text NOT NULL DEFAULT 'yellow',
    source_month text NOT NULL,
    s3_key       text NOT NULL,
    row_count    integer NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (taxi_type, source_month),
    CONSTRAINT manifest_source_month_format CHECK (source_month ~ '^\d{4}-(0[1-9]|1[0-2])$')
);

COMMENT ON TABLE control.manifest IS
    'Idempotency/audit log of which (taxi_type, source_month) partitions have been successfully loaded into raw.trips. Row presence = loaded; mark_loaded() upserts on rerun rather than inserting a duplicate.';

COMMENT ON COLUMN control.manifest.taxi_type IS
    'TLC taxi type loaded, matching the taxi_type param threaded through ingestion/tlc.py and extract.py (currently always "yellow" in this project''s scope).';
COMMENT ON COLUMN control.manifest.source_month IS
    'Partition tag, "YYYY-MM", matching raw.trips.source_month exactly -- this is the natural key ingestion/manifest.py checks and marks against.';
COMMENT ON COLUMN control.manifest.s3_key IS
    'S3 key of the raw parquet object loaded for this partition (return value of extract.stream_tlc_to_s3 / ingestion.tlc.s3_raw_key) -- traces a manifest row back to the exact object it was loaded from.';
COMMENT ON COLUMN control.manifest.row_count IS
    'Row count loaded into raw.trips for this partition (return value of load_raw.load_month) -- cheap sanity check against the source file and against raw.trips itself.';
COMMENT ON COLUMN control.manifest.loaded_at IS
    'When this partition was (most recently) loaded. Updated on rerun via upsert, not appended -- this table holds current state per partition, not a full load history.';
