select customer_unique_id, sum(case when is_current then 1 else 0 end) as n_current
from {{ ref('dim_customer') }} where customer_key <> 'unknown'
group by 1 having sum(case when is_current then 1 else 0 end) <> 1
