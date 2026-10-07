-- One row per order, joining every stage of its lifecycle. The other marts
-- aggregate from here, so business rules are defined once.

with orders as (select * from {{ ref('stg_orders') }}),
     payments as (select * from {{ ref('stg_payments') }}),
     inventory as (select * from {{ ref('stg_inventory') }}),
     outcomes as (select * from {{ ref('stg_order_outcomes') }})

select
    o.order_id,
    o.customer_id,
    o.created_at,
    cast(o.created_at as date)                       as order_date,
    o.total_amount,

    p.payment_result,
    p.payment_amount,
    p.payment_method,
    p.failure_reason,
    p.processed_at,

    i.inventory_result,
    i.reserved_at,
    i.expires_at,

    -- An order with no terminal event is still in flight, not missing.
    coalesce(oc.outcome, 'pending')                  as outcome,
    oc.outcome_at,
    oc.inventory_status_at_outcome,

    date_diff('millisecond', o.created_at, oc.outcome_at) / 1000.0
                                                     as seconds_to_outcome,

    -- Cancelled before the inventory result arrived, yet stock was then
    -- reserved anyway: stock held for an order that no longer exists.
    --
    -- Defined from what order-service knew when it decided (inventory
    -- status null at cancellation) rather than by comparing reserved_at
    -- with outcome_at. Those timestamps come from different services'
    -- clocks, and comparing them would depend on how well-synchronised
    -- the hosts were.
    (
        oc.outcome = 'cancelled'
        and oc.inventory_status_at_outcome is null
        and i.inventory_result = 'inventory_reserved'
    )                                                as stock_reserved_after_cancellation

from orders o
left join payments p   on p.order_id = o.order_id
left join inventory i  on i.order_id = o.order_id
left join outcomes oc  on oc.order_id = o.order_id
