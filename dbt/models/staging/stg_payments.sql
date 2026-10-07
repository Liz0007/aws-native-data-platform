select
    order_id,
    status          as payment_result,
    amount          as payment_amount,
    payment_method,
    failure_reason,
    processed_at
from {{ source('silver', 'silver_payment_processed') }}
