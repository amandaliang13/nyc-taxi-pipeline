-- Static dimension, small lookup.
select
    payment_type_id,
    payment_type_name
from {{ ref('stg_payment_types') }}
