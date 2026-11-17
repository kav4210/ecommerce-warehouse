select customer_unique_id, count(*) filter (where is_current) as n_current
from {{ ref('dim_customer') }} where customer_key <> 'unknown'
group by 1 having count(*) filter (where is_current) <> 1
