{{ config(post_hook=[
    "create index on {{ this }} (seller_key)",
    "analyze {{ this }}"
]) }}
select seller_id as seller_key, city, state, zip_prefix
from {{ ref('stg_sellers') }}
union all
select 'unknown', 'unknown', 'unknown', null
