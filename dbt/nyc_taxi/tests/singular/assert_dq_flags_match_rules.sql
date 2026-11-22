-- The DQ flags stored on fact_trips must match the rules in architecture.md §3,
-- re-derived here from the fact's own columns. is_valid_trip must be true exactly
-- when no rule fires. Failing rows list every mismatched column in failure_reason.
--
-- This is not stg_trips checking itself. fact_trips is incremental, so months built
-- before a rule or var('min_flag_drop_fare') changed keep their old flags. This test
-- catches them; the fix is the README runbook "A DQ rule or threshold changes". The
-- rule logic itself, edge cases included, is unit-tested on stg_trips
-- (models/staging/stg_trips_unit_tests.yml).
-- One scan for every rule: a test per rule would scan the ~110M-row fact once each.
-- Two planner hints keep that scan cheap (measured on 7.5M rows: 14.4s -> 3.6s):
--   - zones is "not materialized". Referenced twice, it would otherwise be
--     materialized, and a materialized CTE rules out a parallel scan of the fact.
--   - rederived ends in "offset 0", so it stays a subquery and each rule is computed
--     once per row. Inlined, every rule is computed again inside the is_valid_trip check.

{{ config(fail_calc='coalesce(sum(bad_trips), 0)') }}

with zones as not materialized (
    select location_id from {{ ref('stg_zones') }}
),

rederived as (
    select
        f.trip_key,
        f.source_month,
        f.is_negative_fare,
        f.is_zero_distance_charged,
        f.is_nonpositive_duration,
        f.is_over_24h,
        f.is_unknown_location,
        f.is_pickup_outside_source_month,
        f.is_outside_nyc,
        f.is_valid_trip,
        f.pickup_location_in_lookup,
        f.dropoff_location_in_lookup,

        pz.location_id is not null as rule_pickup_location_in_lookup,
        dz.location_id is not null as rule_dropoff_location_in_lookup,
        coalesce(f.fare_amount < 0, false) as rule_negative_fare,
        coalesce(
            f.trip_distance = 0 and f.fare_amount > {{ var('min_flag_drop_fare') }},
            false
        ) as rule_zero_distance_charged,
        coalesce(f.dropoff_datetime <= f.pickup_datetime, false) as rule_nonpositive_duration,
        coalesce(f.dropoff_datetime - f.pickup_datetime > interval '24 hours', false) as rule_over_24h,
        -- Missing from the lookup (a NULL id never joins), or 264 ("Unknown").
        pz.location_id is null
            or dz.location_id is null
            or coalesce(264 in (f.pickup_location_id, f.dropoff_location_id), false)
            as rule_unknown_location,
        -- Tolerated: pickups from the day before the month starts to the day after it
        -- ends, i.e. [month start - 1 day, next month start + 1 day). NULL is flagged.
        not coalesce(
            f.pickup_datetime >= to_date(f.source_month, 'YYYY-MM') - interval '1 day'
            and f.pickup_datetime < to_date(f.source_month, 'YYYY-MM') + interval '1 month 1 day',
            false
        ) as rule_pickup_outside_source_month,
        coalesce(265 in (f.pickup_location_id, f.dropoff_location_id), false) as rule_outside_nyc
    from {{ ref('fact_trips') }} as f
    left join zones as pz on pz.location_id = f.pickup_location_id
    left join zones as dz on dz.location_id = f.dropoff_location_id
    offset 0  -- optimization fence, see the header
),

checked as (
    select
        trip_key,
        source_month,
        concat_ws(', ',
            case when is_negative_fare is distinct from rule_negative_fare then 'is_negative_fare' end,
            case when is_zero_distance_charged is distinct from rule_zero_distance_charged then 'is_zero_distance_charged' end,
            case when is_nonpositive_duration is distinct from rule_nonpositive_duration then 'is_nonpositive_duration' end,
            case when is_over_24h is distinct from rule_over_24h then 'is_over_24h' end,
            case when is_unknown_location is distinct from rule_unknown_location then 'is_unknown_location' end,
            case when is_pickup_outside_source_month is distinct from rule_pickup_outside_source_month then 'is_pickup_outside_source_month' end,
            case when is_outside_nyc is distinct from rule_outside_nyc then 'is_outside_nyc' end,
            case when pickup_location_in_lookup is distinct from rule_pickup_location_in_lookup then 'pickup_location_in_lookup' end,
            case when dropoff_location_in_lookup is distinct from rule_dropoff_location_in_lookup then 'dropoff_location_in_lookup' end,
            -- Against the re-derived rules, so a wrong stored flag can't hide here. A
            -- NULL is_valid_trip fails too: it would drop out of both sides of a filter.
            case when is_valid_trip is distinct from not (
                rule_negative_fare
                or rule_zero_distance_charged
                or rule_nonpositive_duration
                or rule_over_24h
                or rule_unknown_location
                or rule_pickup_outside_source_month
            ) then 'is_valid_trip' end
        ) as failure_reason
    from rederived
)

select
    source_month,
    failure_reason,
    count(*)      as bad_trips,
    min(trip_key) as example_trip_key
from checked
where failure_reason <> ''
group by source_month, failure_reason
