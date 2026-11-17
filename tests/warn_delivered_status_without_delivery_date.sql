{{ config(severity='warn') }}
-- known Olist defect, counted not hidden
select order_id from {{ ref('stg_orders') }} where status = 'delivered' and delivered_ts is null
