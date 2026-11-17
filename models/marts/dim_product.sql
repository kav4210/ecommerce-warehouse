{{ config(post_hook=[
    "{{ index_hook('product_key') }}",
    "{{ analyze_hook() }}"
]) }}
select product_id as product_key, category, category_pt, weight_g, photos_qty
from {{ ref('stg_products') }}
union all
select 'unknown', 'unknown', 'unknown', null, null
