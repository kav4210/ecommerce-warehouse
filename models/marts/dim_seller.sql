{{ config(post_hook=[
    "{{ index_hook('seller_key') }}",
    "{{ analyze_hook() }}"
]) }}
select seller_id as seller_key, city, state, zip_prefix
from {{ ref('stg_sellers') }}
union all
select 'unknown', 'unknown', 'unknown', null
