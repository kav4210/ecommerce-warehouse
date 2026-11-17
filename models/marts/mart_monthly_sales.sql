select
    d.year_month,
    count(*)                                              as orders,
    sum(f.items_value)                                    as items_revenue,
    sum(f.freight_value)                                  as freight,
    round(avg(f.items_value + f.freight_value), 2)        as avg_order_value,
    round(100.0 * avg(case when f.is_delivered then (case when f.is_late then 1 else 0 end) end), 2)               as late_pct,
    round(avg(f.review_score), 3)                         as avg_review
from {{ ref('fct_orders') }} f
join {{ ref('dim_date') }} d on d.date_key = f.order_date_key
where f.status not in ('canceled', 'unavailable') and f.item_count > 0
group by 1
