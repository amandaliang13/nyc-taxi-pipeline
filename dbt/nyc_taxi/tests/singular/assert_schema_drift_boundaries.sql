-- The two schema-drift columns on fact_trips must switch on at their source_month
-- boundary and stay on (architecture.md, "Known schema drift"):
--   - cbd_congestion_fee: TLC files carry it from 2025-01 (congestion pricing,
--     effective 2025-01-05).
--   - request_source: TLC files carry it from 2026-06.
-- Before its boundary the column is absent from the file, so a value there means a
-- month landed under the wrong source_month or the boundary is wrong. From the
-- boundary on, a month where the column is NULL on every row means TLC shipped that
-- month without it. For cbd_congestion_fee this is the architecture.md §6
-- "mid-year gap" check.
-- Only whole months fail. A month with some NULLs passes: the file has the column,
-- and its NULLs are missing values, not drift.
-- One scan of the fact for both columns, grouped by source_month.

{%- set cbd_congestion_fee_from = '2025-01' -%}
{%- set request_source_from = '2026-06' %}

with months as (
    select
        source_month,
        count(*)                  as trips,
        count(cbd_congestion_fee) as cbd_congestion_fee_values,
        count(request_source)     as request_source_values
    from {{ ref('fact_trips') }}
    group by source_month
),

checked as (
    select
        *,
        concat_ws(', ',
            case when source_month < '{{ cbd_congestion_fee_from }}' and cbd_congestion_fee_values > 0
                then 'cbd_congestion_fee has values before {{ cbd_congestion_fee_from }}' end,
            case when source_month >= '{{ cbd_congestion_fee_from }}' and cbd_congestion_fee_values = 0
                then 'cbd_congestion_fee all NULL from {{ cbd_congestion_fee_from }} on' end,
            case when source_month < '{{ request_source_from }}' and request_source_values > 0
                then 'request_source has values before {{ request_source_from }}' end,
            case when source_month >= '{{ request_source_from }}' and request_source_values = 0
                then 'request_source all NULL from {{ request_source_from }} on' end
        ) as failure_reason
    from months
)

select
    source_month,
    failure_reason,
    trips,
    cbd_congestion_fee_values,
    request_source_values
from checked
where failure_reason <> ''
