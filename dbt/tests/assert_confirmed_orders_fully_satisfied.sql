-- A confirmed order must have a succeeded payment and reserved stock, read
-- from the payment and inventory topics themselves — not from the statuses
-- order-service copied into its confirmation event. Silver already checks
-- the copy; this checks the copy against the source of truth.

select order_id, payment_result, inventory_result
from {{ ref('fct_orders') }}
where outcome = 'confirmed'
  and (
      payment_result is distinct from 'payment_succeeded'
      or inventory_result is distinct from 'inventory_reserved'
  )
