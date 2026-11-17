{{ config(post_hook=[
    "create index on {{ this }} (order_id)",
    "create index on {{ this }} (customer_key)",
    "create index on {{ this }} (order_date_key)",
    "analyze {{ this }}"
]) }}
with items as (
    select order_id, count(*) as item_count,
           sum(price) as items_value, sum(freight_value) as freight_value
    from {{ ref('stg_order_items') }} group by 1
),
pay as (
    select order_id, sum(payment_value) as payment_value, count(*) as payment_count,
           string_agg(distinct payment_type, ',' order by payment_type) as payment_types
    from {{ ref('stg_payments') }} group by 1
)
select
    o.order_id,
    coalesce(d.customer_key, 'unknown')                         as customer_key,
    to_char(o.purchase_ts, 'YYYYMMDD')::int                     as order_date_key,
    o.status,
    o.purchase_ts, o.approved_ts, o.delivered_ts, o.estimated_ts,
    coalesce(i.item_count, 0)                                   as item_count,
    coalesce(i.items_value, 0)                                  as items_value,
    coalesce(i.freight_value, 0)                                as freight_value,
    coalesce(p.payment_value, 0)                                as payment_value,
    p.payment_types,
    r.review_score,
    (o.delivered_ts is not null)                                as is_delivered,
    case when o.delivered_ts is not null
         then extract(epoch from (o.delivered_ts - o.purchase_ts)) / 86400.0 end as delivery_days,
    case when o.delivered_ts is not null and o.estimated_ts is not null
         then o.delivered_ts::date > o.estimated_ts::date end   as is_late
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c on c.customer_id = o.customer_id
left join {{ ref('dim_customer') }} d
       on d.customer_unique_id = c.customer_unique_id
      and o.purchase_ts >= d.valid_from and o.purchase_ts < d.valid_to
left join items i on i.order_id = o.order_id
left join pay   p on p.order_id = o.order_id
left join {{ ref('stg_reviews') }} r on r.order_id = o.order_id
