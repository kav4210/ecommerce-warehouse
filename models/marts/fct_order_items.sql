{{ config(post_hook=[
    "create index on {{ this }} (order_id)",
    "create index on {{ this }} (product_key)",
    "create index on {{ this }} (seller_key)",
    "analyze {{ this }}"
]) }}
select
    i.order_id,
    i.item_seq,
    coalesce(p.product_key, 'unknown') as product_key,
    coalesce(s.seller_key,  'unknown') as seller_key,
    f.customer_key,
    f.order_date_key,
    f.status,
    i.price,
    i.freight_value,
    i.price + i.freight_value          as line_total
from {{ ref('stg_order_items') }} i
join {{ ref('fct_orders') }} f using (order_id)
left join {{ ref('dim_product') }} p on p.product_key = i.product_id
left join {{ ref('dim_seller') }}  s on s.seller_key  = i.seller_id
