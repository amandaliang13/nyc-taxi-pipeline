-- Vendor (TPEP provider) lookup (codes per the raw.trips.vendor_id comment).
select
    vendor_id::smallint as vendor_id,
    vendor_name
from {{ ref('vendors') }}
