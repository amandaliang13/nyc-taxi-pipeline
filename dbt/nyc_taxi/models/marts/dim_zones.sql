-- Zone dimension, SCD Type 2 on Congestion Relief Zone (CRZ) membership.
-- Built from an event table rather than a dbt snapshot: TLC's zone lookup is a single
-- undated file overwritten in place, so it carries no history to observe. CRZ
-- membership is a real, dated change (MTA tolling from 2025-01-05). Rebuilding from
-- events is deterministic and full-refresh safe.
-- Zone name, borough and service_zone are Type 1: every version shows today's value.
-- Changing seeds/congestion_relief_zone.csv invalidates fact_trips' version keys;
-- follow the README runbook.

with events as (
    -- Baseline: every zone starts outside the CRZ.
    select
        location_id,
        '1900-01-01'::timestamp as valid_from,
        false as is_in_congestion_relief_zone
    from {{ ref('stg_zones') }}

    union all

    select
        location_id::smallint,
        effective_from::timestamp,
        true
    from {{ ref('congestion_relief_zone') }}
),

versions as (
    select
        *,
        coalesce(
            lead(valid_from) over (partition by location_id order by valid_from),
            '9999-12-31'::timestamp
        ) as valid_to
    from events
)

select
    -- valid_from is formatted explicitly: a plain text cast follows the session's
    -- DateStyle, which would change every key (and orphan fact_trips' copies).
    {{ dbt_utils.generate_surrogate_key([
        'v.location_id',
        "to_char(v.valid_from, 'YYYY-MM-DD HH24:MI:SS')",
    ]) }} as zone_version_key,
    v.location_id,
    z.borough,
    z.zone_name,
    z.service_zone,
    v.is_in_congestion_relief_zone,
    v.valid_from,
    v.valid_to,
    v.valid_to = '9999-12-31'::timestamp as is_current
from versions as v
inner join {{ ref('stg_zones') }} as z using (location_id)
