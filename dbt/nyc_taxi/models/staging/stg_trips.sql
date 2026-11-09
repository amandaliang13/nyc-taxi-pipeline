-- Rename, type, and DQ-flag raw trips; no aggregation.
-- Rows are flagged, never dropped: one boolean per rule keeps each exclusion
-- auditable, and is_valid_trip = no rule fired.
-- cbd_congestion_fee is NULL before source_month 2025-01 and request_source before
-- 2026-06 because TLC didn't ship those columns yet. That's schema drift, not a DQ
-- defect, so no rule uses them.

with source as (
    select * from {{ source('raw', 'trips') }}
),

zones as (
    select location_id from {{ ref('stg_zones') }}
),

renamed as (
    select
        -- Stable trip identity: the row's position in its month's TLC file. Survives
        -- reloads of the same file; a TLC republish of the month may renumber it.
        {{ dbt_utils.generate_surrogate_key(['source_month', 'source_row_number']) }} as trip_key,
        id as raw_trip_id,  -- technical id; regenerated on every reload of the month
        source_month,
        source_row_number,
        vendor_id,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        tpep_pickup_datetime::date as pickup_date,
        tpep_dropoff_datetime - tpep_pickup_datetime as trip_duration,
        passenger_count,
        trip_distance,
        ratecode_id,
        case store_and_fwd_flag when 'Y' then true when 'N' then false end as is_store_and_forward,
        pu_location_id as pickup_location_id,
        do_location_id as dropoff_location_id,
        payment_type as payment_type_id,
        request_source,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        total_amount
    from source
),

located as (
    select
        *,
        coalesce(pickup_location_id in (select location_id from zones), false) as pickup_location_in_lookup,
        coalesce(dropoff_location_id in (select location_id from zones), false) as dropoff_location_in_lookup
    from renamed
),

flagged as (
    select
        *,
        coalesce(fare_amount < 0, false) as is_negative_fare,
        coalesce(
            trip_distance = 0 and fare_amount > {{ var('min_flag_drop_fare') }},
            false
        ) as is_zero_distance_charged,
        coalesce(dropoff_datetime <= pickup_datetime, false) as is_nonpositive_duration,
        coalesce(trip_duration > interval '24 hours', false) as is_over_24h,
        -- Zone 264 is the lookup's "Unknown" placeholder row: an id in the lookup that
        -- still names no place, so it counts as unknown alongside ids missing from it.
        (
            not pickup_location_in_lookup
            or not dropoff_location_in_lookup
            or coalesce(pickup_location_id = 264 or dropoff_location_id = 264, false)
        ) as is_unknown_location,
        -- A pickup outside its file's month is a DQ problem in its own right (TLC files
        -- carry stray rows, some decades off). A day either side is tolerated for trips
        -- crossing a month boundary. A missing pickup can't be placed, so it is flagged.
        coalesce(
            not (
                pickup_date >= to_date(source_month, 'YYYY-MM') - 1
                and pickup_date <= (to_date(source_month, 'YYYY-MM') + interval '1 month')::date
            ),
            true
        ) as is_pickup_outside_source_month,
        -- Zone 265 is the lookup's "Outside of NYC" row. Informational only, not a DQ
        -- rule: trips to Newark or Westchester are real trips.
        coalesce(pickup_location_id = 265 or dropoff_location_id = 265, false) as is_outside_nyc
    from located
)

select
    *,
    not (
        is_negative_fare
        or is_zero_distance_charged
        or is_nonpositive_duration
        or is_over_24h
        or is_unknown_location
        or is_pickup_outside_source_month
    ) as is_valid_trip
from flagged
