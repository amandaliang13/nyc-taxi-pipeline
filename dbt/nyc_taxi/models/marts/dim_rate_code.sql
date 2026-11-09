-- Static dimension: TLC RatecodeID values (seeds/rate_codes.csv).
select
    rate_code_id,
    rate_code_name
from {{ ref('stg_rate_codes') }}
