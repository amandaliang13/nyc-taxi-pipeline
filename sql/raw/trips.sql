-- raw.trips: unmodified landing table for NYC TLC yellow taxi monthly Parquet files.
-- Loaded by ingestion/load_raw.py; no cleaning/filtering -- DQ flagging happens in dbt staging.
-- id is a technical PK only, regenerated on every reload -- idempotency comes from
-- load_month()'s DELETE+COPY per source_month. The stable row identity is
-- (source_month, source_row_number): the row's position in the TLC file.
-- Schema drift: cbd_congestion_fee is absent from the source entirely before source_month
-- 2025-01, not just null-valued. Same for request_source before source_month 2026-06.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.trips (
    id                      bigserial PRIMARY KEY,
    vendor_id               smallint,
    tpep_pickup_datetime    timestamp without time zone,
    tpep_dropoff_datetime   timestamp without time zone,
    passenger_count         smallint,
    trip_distance           numeric(9, 2),
    ratecode_id             smallint,
    store_and_fwd_flag      text,
    pu_location_id          smallint,
    do_location_id          smallint,
    payment_type            smallint,
    fare_amount             numeric(10, 2),
    extra                   numeric(10, 2),
    mta_tax                 numeric(10, 2),
    tip_amount              numeric(10, 2),
    tolls_amount            numeric(10, 2),
    improvement_surcharge   numeric(10, 2),
    total_amount            numeric(10, 2),
    congestion_surcharge    numeric(10, 2),
    airport_fee             numeric(10, 2),
    cbd_congestion_fee      numeric(10, 2),
    request_source          text,
    source_month            text NOT NULL,
    source_row_number       integer NOT NULL
);

-- CREATE TABLE IF NOT EXISTS won't touch a warehouse built before request_source existed.
ALTER TABLE raw.trips ADD COLUMN IF NOT EXISTS request_source text;
-- Same for source_row_number. It is nullable here because existing rows have no file
-- position; they stay NULL until their month is reloaded (scripts/backfill.sh).
ALTER TABLE raw.trips ADD COLUMN IF NOT EXISTS source_row_number integer;

-- Natural key: one row per position in each month's file. Its leading source_month
-- column also backs load_raw.py's DELETE-by-partition and dbt's source_month filters.
CREATE UNIQUE INDEX IF NOT EXISTS uq_trips_source_row ON raw.trips (source_month, source_row_number);
-- The single-column index it replaces is redundant; drop it from older warehouses.
DROP INDEX IF EXISTS raw.idx_trips_source_month;

COMMENT ON TABLE raw.trips IS
    'TLC yellow taxi trips, landed unmodified from S3 raw/. source_month is appended on load, not part of the source.';

COMMENT ON COLUMN raw.trips.vendor_id IS
    'Provider code: 1=Creative Mobile Technologies, 2=Curb Mobility, 6=Myle Technologies, 7=Helix.';
COMMENT ON COLUMN raw.trips.ratecode_id IS
    'Rate code: 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride, 99=Unknown.';
COMMENT ON COLUMN raw.trips.payment_type IS
    'Payment code: 0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided.';
COMMENT ON COLUMN raw.trips.cbd_congestion_fee IS
    'MTA Congestion Relief Zone charge, effective 2025-01-05. NULL before source_month 2025-01 by schema absence, not a data quality issue.';
COMMENT ON COLUMN raw.trips.request_source IS
    'Trip request channel code (e.g. HV0003, HV0005, A, CC), added by TLC from 2026-06. NULL before source_month 2026-06 by schema absence, not a data quality issue.';
COMMENT ON COLUMN raw.trips.source_month IS
    'Ingestion partition tag "YYYY-MM", appended by load_raw.py. Basis for idempotent reload and downstream incremental filtering.';
COMMENT ON COLUMN raw.trips.source_row_number IS
    '1-based row position in the TLC Parquet file, appended by load_raw.py. Stable across reloads of the same file; a TLC republish of the month may renumber it.';
