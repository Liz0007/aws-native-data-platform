-- An order is confirmed or cancelled, never both. Each topic is unique on
-- order_id on its own (see sources.yml); this checks across the two.

select c.order_id
from {{ source('silver', 'silver_order_confirmed') }} c
join {{ source('silver', 'silver_order_cancelled') }} x
  on x.order_id = c.order_id
