-- The amount charged must equal the order total. A mismatch means the
-- payment service charged a different figure than the order recorded.

select order_id, total_amount, payment_amount
from {{ ref('fct_orders') }}
where payment_amount is not null
  and payment_amount <> total_amount
