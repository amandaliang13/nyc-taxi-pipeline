-- Rename, type, and DQ-flag raw trips; no aggregation.
-- Rows are flagged, never dropped: one boolean per rule keeps each exclusion
-- auditable, and is_valid_trip = no rule fired.
-- cbd_congestion_fee is NULL before source_month 2025-01 because the column
-- didn't exist yet. That's schema drift, not a DQ defect, so no rule uses it.

with source as (
    select * from {{ source('raw', 'trips') }}
),

zones as (
    select location_id from {{ ref('stg_zones') }}
),

renamed as (
    select
        id as raw_trip_id,  -- technical id; regenerated on every reload of the month
        source_month,
        vendor_id,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        tpep_dropoff_datetime - tpep_pickup_datetime as trip_duration,
        passenger_count,
        trip_distance,
        ratecode_id,
        case store_and_fwd_flag when 'Y' then true when 'N' then false end as is_store_and_forward,
        pu_location_id as pickup_location_id,
        do_location_id as dropoff_location_id,
        payment_type as payment_type_id,
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
        not (
            coalesce(pickup_location_id in (select location_id from zones), false)
            and coalesce(dropoff_location_id in (select location_id from zones), false)
        ) as is_unknown_location
    from renamed
)

select
    *,
    not (
        is_negative_fare
        or is_zero_distance_charged
        or is_nonpositive_duration
        or is_over_24h
        or is_unknown_location
    ) as is_valid_trip
from flagged
