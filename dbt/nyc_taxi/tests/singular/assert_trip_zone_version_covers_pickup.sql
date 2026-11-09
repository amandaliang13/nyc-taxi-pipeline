-- Every zone version key on fact_trips must exist in dim_zones and point at the
-- version whose [valid_from, valid_to) covers the trip's pickup time.
-- The coverage check is the only test that catches a key pointing at the wrong
-- version, e.g. after a CRZ event is added to the seed without rebuilding the fact
-- (see the README runbook).

with keys as (
    select trip_key, source_month, 'pickup' as role, pickup_zone_version_key as zone_version_key, pickup_datetime
    from {{ ref('fact_trips') }}
    where pickup_zone_version_key is not null

    union all

    select trip_key, source_month, 'dropoff', dropoff_zone_version_key, pickup_datetime
    from {{ ref('fact_trips') }}
    where dropoff_zone_version_key is not null
)

select
    k.trip_key,
    k.source_month,
    k.role,
    k.zone_version_key,
    k.pickup_datetime,
    z.valid_from,
    z.valid_to,
    case
        when z.zone_version_key is null then 'key not in dim_zones'
        else 'version dates do not cover pickup'
    end as failure_reason
from keys as k
left join {{ ref('dim_zones') }} as z using (zone_version_key)
where z.zone_version_key is null
    or not (k.pickup_datetime >= z.valid_from and k.pickup_datetime < z.valid_to)
