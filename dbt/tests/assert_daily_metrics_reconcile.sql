-- The daily rollup must account for every order in the fact table. Returns
-- a row only when the totals disagree.

with fact as (
    select count(*) as n from {{ ref('fct_orders') }}
),
daily as (
    select sum(orders) as n from {{ ref('daily_order_metrics') }}
)
select fact.n as fact_orders, daily.n as daily_orders
from fact cross join daily
where fact.n <> daily.n
