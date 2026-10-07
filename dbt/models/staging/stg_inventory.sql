select
    order_id,
    status       as inventory_result,
    reserved_at,
    expires_at
from {{ source('silver', 'silver_inventory_reserved') }}
