with s as (
    select
        i.seller_key,
        count(*)                          as items_sold,
        count(distinct i.order_id)        as orders,
        sum(i.price)                      as revenue,
        round(100.0 * avg(case when o.is_delivered then (case when o.is_late then 1 else 0 end) end), 2) as late_pct,
        round(avg(o.review_score), 3)     as avg_review
    from {{ ref('fct_order_items') }} i
    join {{ ref('fct_orders') }} o using (order_id)
    where i.status not in ('canceled', 'unavailable')
    group by 1
)
select s.*, d.state,
       rank() over (order by revenue desc)                               as revenue_rank,
       round({{ to_numeric('100 * cume_dist() over (order by revenue)') }}, 1)    as revenue_percentile
from s join {{ ref('dim_seller') }} d on d.seller_key = s.seller_key
