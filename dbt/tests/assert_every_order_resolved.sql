-- Every order should reach a terminal state. The most recent few minutes
-- are excluded: an order created just before the data was cut can
-- legitimately still be in flight, and that is not a failure.

with latest as (
    select max(created_at) as max_created from {{ ref('fct_orders') }}
)
select f.order_id, f.created_at
from {{ ref('fct_orders') }} f
cross join latest
where f.outcome = 'pending'
  and f.created_at < latest.max_created - interval '5' minute
