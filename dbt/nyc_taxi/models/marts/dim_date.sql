-- Date dimension, one row per calendar day, keyed on the date itself (Postgres has a
-- native date type; no yyyymmdd surrogate needed). Day grain: hour-of-day comes from
-- fact_trips.pickup_datetime.
-- Spine bounds are vars (dbt_project.yml), so loading a new year means widening the
-- vars and regenerating seeds/us_holidays.csv, not editing SQL.

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date="cast('" ~ var('dim_date_end') ~ "' as date)"
    ) }}
),

days as (
    select date_day::date as date_day from spine
)

select
    d.date_day,
    extract(year from d.date_day)::smallint as year,
    extract(quarter from d.date_day)::smallint as quarter,
    extract(month from d.date_day)::smallint as month,
    to_char(d.date_day, 'FMMonth') as month_name,
    extract(day from d.date_day)::smallint as day_of_month,
    extract(isodow from d.date_day)::smallint as iso_day_of_week,
    to_char(d.date_day, 'FMDay') as day_name,
    extract(isodow from d.date_day) in (6, 7) as is_weekend,
    h.holiday_date is not null as is_holiday,
    h.holiday_name
from days as d
left join {{ ref('us_holidays') }} as h on h.holiday_date = d.date_day
