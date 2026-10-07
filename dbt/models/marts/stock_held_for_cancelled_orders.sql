-- Cancelled orders whose stock was reserved anyway.
--
-- Payment and inventory are processed in parallel upstream. When payment
-- fails first, the order is cancelled before inventory responds, and the
-- reservation that follows holds stock for an order that no longer exists.
-- The upstream platform documents releasing expired reservations as
-- deferred work; this table measures what that gap holds.

select
    order_id,
    created_at,
    outcome_at                         as cancelled_at,
    reserved_at,
    expires_at,
    total_amount,
    stock_reserved_after_cancellation,
    failure_reason
from {{ ref('fct_orders') }}
where outcome = 'cancelled'
  and inventory_result = 'inventory_reserved'
