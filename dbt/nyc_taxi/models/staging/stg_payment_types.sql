-- Payment type lookup (codes per the raw.trips.payment_type comment).
select
    payment_type_id::smallint as payment_type_id,
    payment_type_name
from {{ ref('payment_types') }}
