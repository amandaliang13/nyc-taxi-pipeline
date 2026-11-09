-- Rate code lookup (codes per the TLC data dictionary; see seeds/schema.yml).
select
    rate_code_id::smallint as rate_code_id,
    rate_code_name
from {{ ref('rate_codes') }}
