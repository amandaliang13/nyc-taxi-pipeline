-- TLC zone lookup, renamed. Feeds the location DQ rule in stg_trips and the
-- dim_zones snapshot.
select
    locationid::smallint as location_id,
    borough,
    zone as zone_name,
    service_zone
from {{ ref('taxi_zone_lookup') }}
