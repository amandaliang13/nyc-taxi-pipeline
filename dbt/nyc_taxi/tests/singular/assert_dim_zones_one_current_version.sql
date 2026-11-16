-- dim_zones SCD2 ends, per lookup zone: the history starts at the 1900-01-01
-- baseline and exactly one version is current. With mutually_exclusive_ranges on
-- dim_zones (no overlaps, no gaps; models/marts/schema.yml), this means every pickup
-- time maps to exactly one version of every zone.
-- Starts from stg_zones, so a zone missing from dim_zones fails with 0 current versions.

select
    z.location_id,
    count(*) filter (where d.is_current) as current_versions,
    min(d.valid_from) as first_valid_from
from {{ ref('stg_zones') }} as z
left join {{ ref('dim_zones') }} as d using (location_id)
group by z.location_id
having count(*) filter (where d.is_current) <> 1
    or min(d.valid_from) is distinct from '1900-01-01'::timestamp
