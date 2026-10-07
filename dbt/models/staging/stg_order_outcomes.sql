-- Confirmed and cancelled are separate topics upstream but one concept
-- here: the order's terminal state. The payment and inventory statuses are
-- what order-service knew at the moment it decided, which is not always
-- the final state — see stock_held_for_cancelled_orders.

select
    order_id,
    'confirmed'       as outcome,
    event_timestamp   as outcome_at,
    payment_status    as payment_status_at_outcome,
    inventory_status  as inventory_status_at_outcome
from {{ source('silver', 'silver_order_confirmed') }}

union all

select
    order_id,
    'cancelled'       as outcome,
    event_timestamp   as outcome_at,
    payment_status    as payment_status_at_outcome,
    inventory_status  as inventory_status_at_outcome
from {{ source('silver', 'silver_order_cancelled') }}
