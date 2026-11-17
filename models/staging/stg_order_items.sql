select
    order_id,
    order_item_id::int                         as item_seq,
    product_id,
    seller_id,
    nullif(shipping_limit_date, '')::timestamp as shipping_limit_ts,
    price::numeric(12,2)                       as price,
    freight_value::numeric(12,2)               as freight_value
from {{ source('raw', 'order_items') }}
