{{ config(severity='warn') }}
select order_id from {{ ref('stg_orders') }} where delivered_ts < purchase_ts
