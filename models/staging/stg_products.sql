select
    p.product_id,
    coalesce(nullif(p.product_category_name, ''), 'unknown') as category_pt,
    coalesce(t.product_category_name_english,
             nullif(p.product_category_name, ''), 'unknown') as category,
    {{ to_numeric("nullif(p.product_weight_g, '')") }}      as weight_g,
    nullif(p.product_photos_qty, '')::int                    as photos_qty
from {{ source('raw', 'products') }} p
left join {{ source('raw', 'category_translation') }} t
       on t.product_category_name = p.product_category_name
