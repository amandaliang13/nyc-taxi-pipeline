-- Trip fact, one row per raw trip (valid and invalid; consumers filter on
-- is_valid_trip, and the per-rule flags keep every exclusion auditable here).
--
-- Incremental by source_month: whole-month delete+insert, not a row-level merge.
-- TLC does republish files (errata, the 2022 Parquet re-release), so a month is the
-- unit of change, and trip rows have no key that survives a republish -- a row-level
-- merge would have nothing reliable to match on. Replacing the month mirrors
-- load_raw's DELETE+COPY.
--   - Keyed on source_month, not pickup month: TLC files carry stray pickups from
--     other months (see is_pickup_outside_source_month).
--   - No max(pickup_datetime) watermark: loading month N after N+2 would be skipped.
--   - A month is (re)built when control.manifest.loaded_at differs from the
--     loaded_at it was last built from, so reloading a month (scripts/backfill.sh,
--     e.g. after a TLC republish) propagates on the next run. A month without a
--     manifest row isn't built: mark_loaded runs only after raw.trips commits.
--   - var('fact_rebuild_from') forces a rebuild from that month on, for changes the
--     manifest can't see (a CRZ seed change, see the README runbook).
--
-- Zone version keys are resolved here, at load, by pickup time for both pickup and
-- dropoff: a trip's zone versions are fixed when the trip starts. They are NULL when
-- is_pickup_outside_source_month: a pickup that far from its file's month (some are
-- years off) can't be trusted to pick a version. This also bounds which months a CRZ
-- seed change can affect -- every keyed trip's pickup is within a day of its
-- source_month (see the README runbook).
--
-- on_schema_change='fail': a column added to the select must not be silently
-- dropped on incremental runs. Add it with --full-refresh.

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_month',
        on_schema_change='fail',
        indexes=[
            {'columns': ['source_month', 'raw_loaded_at']},
            {'columns': ['pickup_datetime']},
            {'columns': ['trip_key'], 'unique': true},
        ],
    )
}}

{#- fact_rebuild_from is compared to source_month as a string, so anything but a
    zero-padded YYYY-MM ('2024-1') would silently pick the wrong months. -#}
{%- set rebuild_from = var('fact_rebuild_from') | string -%}
{%- if not modules.re.match('^[0-9]{4}-(0[1-9]|1[0-2])$', rebuild_from) -%}
    {{ exceptions.raise_compiler_error(
        "fact_rebuild_from must be 'YYYY-MM' (zero-padded month), got '" ~ rebuild_from ~ "'"
    ) }}
{%- endif %}

with manifest as (
    -- (taxi_type, source_month) is the manifest PK, so this is one row per month
    -- (asserted by the source test); without the taxi_type filter a second taxi
    -- type would duplicate every trip.
    select source_month, loaded_at
    from {{ source('control', 'manifest') }}
    where taxi_type = 'yellow'
),

months_to_build as (
    select manifest.source_month, manifest.loaded_at
    from manifest
    {% if is_incremental() %}
        left join (
            select source_month, max(raw_loaded_at) as built_from_loaded_at
            from {{ this }}
            group by source_month
        ) as built using (source_month)
        -- "distinct", not "newer": a reload whose loaded_at isn't later (clock step
        -- back, manifest restored or fixed by hand) must still rebuild. A month never
        -- built has a NULL built_from_loaded_at, which is distinct from any loaded_at.
        where manifest.loaded_at is distinct from built.built_from_loaded_at
            or manifest.source_month >= '{{ rebuild_from }}'
    {% endif %}
),

zones as (
    select zone_version_key, location_id, valid_from, valid_to
    from {{ ref('dim_zones') }}
)

select
    t.trip_key,
    t.source_month,
    t.source_row_number,
    m.loaded_at as raw_loaded_at,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_date,
    t.pickup_location_id,
    t.dropoff_location_id,
    pz.zone_version_key as pickup_zone_version_key,
    dz.zone_version_key as dropoff_zone_version_key,
    t.vendor_id,
    t.payment_type_id,
    t.ratecode_id,
    t.request_source,
    t.passenger_count,
    t.trip_distance,
    t.trip_duration,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.total_amount,
    t.is_negative_fare,
    t.is_zero_distance_charged,
    t.is_nonpositive_duration,
    t.is_over_24h,
    t.is_unknown_location,
    t.is_pickup_outside_source_month,
    t.is_valid_trip,
    t.is_outside_nyc,
    t.pickup_location_in_lookup,
    t.dropoff_location_in_lookup
from {{ ref('stg_trips') }} as t
inner join months_to_build as m using (source_month)
left join zones as pz
    on pz.location_id = t.pickup_location_id
    and t.pickup_datetime >= pz.valid_from
    and t.pickup_datetime < pz.valid_to
    and not t.is_pickup_outside_source_month
left join zones as dz
    on dz.location_id = t.dropoff_location_id
    and t.pickup_datetime >= dz.valid_from
    and t.pickup_datetime < dz.valid_to
    and not t.is_pickup_outside_source_month
