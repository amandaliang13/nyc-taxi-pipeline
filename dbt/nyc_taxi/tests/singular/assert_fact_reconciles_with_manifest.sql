-- Per source_month, fact_trips must hold exactly the row_count control.manifest
-- recorded, built from the same load. Rows are counted per (source_month, loaded_at):
-- fact_trips.raw_loaded_at is the manifest loaded_at the month was built from. A side
-- with no match counts as 0 rows, so these fail too:
--   - a manifest month the fact never built;
--   - a stale month: reloaded since the fact was built, whatever its row count. It
--     shows as two rows, one per loaded_at, and clears on the next dbt run;
--   - an orphan: a fact month whose manifest row is gone. fact_trips never deletes
--     it; clear it with dbt run -s fact_trips --full-refresh.
-- The manifest is filtered to yellow, the only taxi type fact_trips builds.

with manifest as (
    select source_month, loaded_at, row_count
    from {{ source('control', 'manifest') }}
    where taxi_type = 'yellow'
),

fact_months as (
    select source_month, raw_loaded_at as loaded_at, count(*) as fact_rows
    from {{ ref('fact_trips') }}
    group by source_month, raw_loaded_at
)

select
    source_month,
    loaded_at,
    m.row_count as manifest_row_count,
    f.fact_rows
from manifest as m
full outer join fact_months as f using (source_month, loaded_at)
where coalesce(m.row_count, 0) <> coalesce(f.fact_rows, 0)
