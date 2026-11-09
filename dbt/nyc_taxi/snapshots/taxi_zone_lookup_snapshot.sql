-- Audit trail of the committed zone lookup seed (seeds/taxi_zone_lookup.csv).
-- It records changes to the *committed* seed. It only captures a TLC revision if
-- someone re-downloads the file and commits it: TLC overwrites the file in place
-- with no effective dates, so dbt_valid_from is when the change was seen here, not
-- when TLC made it. Nothing joins to this; dim_zones is built from events instead.

{% snapshot taxi_zone_lookup_snapshot %}

{{
    config(
        unique_key='location_id',
        strategy='check',
        check_cols=['borough', 'zone_name', 'service_zone'],
        hard_deletes='invalidate',
    )
}}

select * from {{ ref('stg_zones') }}

{% endsnapshot %}
