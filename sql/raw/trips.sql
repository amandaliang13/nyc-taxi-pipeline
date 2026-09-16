-- raw.trips: unmodified landing table for NYC TLC yellow taxi monthly Parquet files.
--
-- Written by ingestion/load_raw.py (DELETE by source_month + COPY, see load_month()).
-- Column names are snake_cased on load (see _COLUMN_RENAMES in load_raw.py) but values
-- and types are otherwise untouched -- no cleaning, no filtering, no dropped rows.
-- Data quality flagging (is_valid_trip) happens downstream in dbt staging (stg_trips).
--
-- Idempotency: load_month() deletes existing rows for a source_month before re-COPYing
-- it, so there is no unique/primary-key constraint needed to prevent duplicate loads.
--
-- Schema drift: cbd_congestion_fee does not exist in the TLC parquet before 2025-01
-- (MTA Congestion Relief Zone charge, effective 2025-01-05). load_raw.py's COPY column
-- list is built from whatever columns are present in a given month's DataFrame, so for
-- source_month < '2025-01' this column is simply omitted from the INSERT and lands NULL.

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

-- Backs both load_raw.py's DELETE-by-partition and dbt's incremental/source-month filters.
CREATE INDEX IF NOT EXISTS idx_trips_source_month ON raw.trips (source_month);

COMMENT ON TABLE raw.trips IS
    'Unmodified TLC yellow taxi monthly trip records, landed as-is from S3 raw/. One row per parquet row; no cleaning or filtering. source_month is appended on load, not part of the TLC source.';

COMMENT ON COLUMN raw.trips.vendor_id IS
    'TPEP provider code: 1=Creative Mobile Technologies LLC, 2=Curb Mobility LLC, 6=Myle Technologies Inc, 7=Helix.';
COMMENT ON COLUMN raw.trips.tpep_pickup_datetime IS
    'Date/time the meter was engaged.';
COMMENT ON COLUMN raw.trips.tpep_dropoff_datetime IS
    'Date/time the meter was disengaged.';
COMMENT ON COLUMN raw.trips.passenger_count IS
    'Number of passengers in the vehicle (driver-entered value).';
COMMENT ON COLUMN raw.trips.trip_distance IS
    'Elapsed trip distance in miles, per taximeter.';
COMMENT ON COLUMN raw.trips.ratecode_id IS
    'Final rate code in effect at trip end: 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated fare, 6=Group ride, 99=Null/unknown.';
COMMENT ON COLUMN raw.trips.store_and_fwd_flag IS
    'Y = trip record held in vehicle memory before sending to vendor ("store and forward"); N = not.';
COMMENT ON COLUMN raw.trips.pu_location_id IS
    'TLC taxi zone where the meter was engaged (source column: PULocationID).';
COMMENT ON COLUMN raw.trips.do_location_id IS
    'TLC taxi zone where the meter was disengaged (source column: DOLocationID).';
COMMENT ON COLUMN raw.trips.payment_type IS
    'Payment method code: 0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip.';
COMMENT ON COLUMN raw.trips.fare_amount IS
    'Time-and-distance fare calculated by the meter.';
COMMENT ON COLUMN raw.trips.extra IS
    'Miscellaneous extras and surcharges.';
COMMENT ON COLUMN raw.trips.mta_tax IS
    'Tax automatically triggered based on the metered rate in use.';
COMMENT ON COLUMN raw.trips.tip_amount IS
    'Tip amount; auto-populated for credit card tips only, cash tips are not included.';
COMMENT ON COLUMN raw.trips.tolls_amount IS
    'Total of all tolls paid in the trip.';
COMMENT ON COLUMN raw.trips.improvement_surcharge IS
    'Flag-drop improvement surcharge, levied since 2015.';
COMMENT ON COLUMN raw.trips.total_amount IS
    'Total amount charged to passengers; does not include cash tips.';
COMMENT ON COLUMN raw.trips.congestion_surcharge IS
    'Total NYS congestion surcharge collected for the trip.';
COMMENT ON COLUMN raw.trips.airport_fee IS
    'Fee for pickups at LaGuardia or JFK airports only.';
COMMENT ON COLUMN raw.trips.cbd_congestion_fee IS
    'MTA Congestion Relief Zone per-trip charge, effective 2025-01-05 onward. NULL for source_month before 2025-01, by schema absence, not a data quality issue.';
COMMENT ON COLUMN raw.trips.source_month IS
    'Ingestion partition tag, "YYYY-MM", appended by load_raw.py -- not present in the TLC source file. Basis for idempotent reload (DELETE+COPY) and for dbt incremental filtering downstream.';
