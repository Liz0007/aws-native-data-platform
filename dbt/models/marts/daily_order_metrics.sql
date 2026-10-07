select
    order_date,
    count(*)                                              as orders,
    count_if(outcome = 'confirmed')                       as confirmed,
    count_if(outcome = 'cancelled')                       as cancelled,
    count_if(outcome = 'pending')                         as pending,

    round(
        cast(count_if(outcome = 'cancelled') as double)
        / nullif(count_if(outcome <> 'pending'), 0),
        4
    )                                                     as cancellation_rate,

    -- Revenue counts confirmed orders only: a cancelled order was never paid.
    sum(case when outcome = 'confirmed' then total_amount end)
                                                          as confirmed_revenue,
    sum(case when outcome = 'cancelled' then total_amount end)
                                                          as cancelled_value,
    round(avg(case when outcome = 'confirmed' then total_amount end), 2)
                                                          as avg_confirmed_order_value,
    round(approx_percentile(seconds_to_outcome, 0.5), 3)  as median_seconds_to_outcome,
    round(approx_percentile(seconds_to_outcome, 0.95), 3) as p95_seconds_to_outcome

from {{ ref('fct_orders') }}
group by order_date
