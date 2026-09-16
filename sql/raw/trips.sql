-- raw.trips: unmodified landing table for NYC TLC yellow taxi monthly Parquet files.
-- Loaded by ingestion/load_raw.py; no cleaning/filtering -- DQ flagging happens in dbt staging.
-- id is a technical PK only -- idempotency comes from load_month()'s DELETE+COPY per
-- source_month, not a uniqueness constraint on the data columns.
-- Schema drift: cbd_congestion_fee is absent from the source entirely before source_month
-- 2025-01, not just null-valued.

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
    source_month            text NOT NULL
);

-- Backs both load_raw.py's DELETE-by-partition and dbt's source_month filters.
CREATE INDEX IF NOT EXISTS idx_trips_source_month ON raw.trips (source_month);

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
COMMENT ON COLUMN raw.trips.source_month IS
    'Ingestion partition tag "YYYY-MM", appended by load_raw.py. Basis for idempotent reload and downstream incremental filtering.';
