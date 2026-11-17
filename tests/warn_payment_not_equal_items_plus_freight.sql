{{ config(severity='warn') }}
select order_id from {{ ref('fct_orders') }}
where item_count > 0 and abs(payment_value - (items_value + freight_value)) > 0.01
