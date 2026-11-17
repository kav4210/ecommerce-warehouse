select
    p.category,
    count(*)                         as items_sold,
    sum(i.price)                     as revenue,
    round(avg(i.price), 2)           as avg_price,
    round(avg(o.review_score), 3)    as avg_review
from {{ ref('fct_order_items') }} i
join {{ ref('dim_product') }} p on p.product_key = i.product_key
join {{ ref('fct_orders') }} o using (order_id)
where i.status not in ('canceled', 'unavailable')
group by 1
