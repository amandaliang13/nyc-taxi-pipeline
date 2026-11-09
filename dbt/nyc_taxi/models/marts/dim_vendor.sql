-- Static dimension, trivial but included for completeness.
select
    vendor_id,
    vendor_name
from {{ ref('stg_vendors') }}
