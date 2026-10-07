-- Why payments fail, and what each reason costs in order value.

with failed as (
    select *
    from {{ ref('fct_orders') }}
    where payment_result = 'payment_failed'
)

select
    failure_reason,
    payment_method,
    count(*)                                                      as failures,
    round(cast(count(*) as double) / sum(count(*)) over (), 4)    as share_of_failures,
    sum(total_amount)                                             as lost_order_value
from failed
group by failure_reason, payment_method
