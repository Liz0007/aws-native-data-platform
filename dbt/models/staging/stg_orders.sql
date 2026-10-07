select
    order_id,
    customer_id,
    total_amount,
    event_timestamp as created_at
from {{ source('silver', 'silver_order_created') }}
