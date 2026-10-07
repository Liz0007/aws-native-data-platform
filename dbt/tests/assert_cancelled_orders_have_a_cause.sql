-- A cancellation needs a reason: either the payment failed or the stock was
-- unavailable. A cancelled order whose payment succeeded and whose stock
-- was reserved would mean order-service cancelled without cause.

select order_id, payment_result, inventory_result
from {{ ref('fct_orders') }}
where outcome = 'cancelled'
  and payment_result is distinct from 'payment_failed'
  and inventory_result is distinct from 'inventory_unavailable'
