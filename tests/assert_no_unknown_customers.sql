select order_id from {{ ref('fct_orders') }} where customer_key = 'unknown'
