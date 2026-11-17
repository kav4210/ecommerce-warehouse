select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix::int as zip_prefix,
    lower(trim(customer_city))    as city,
    upper(trim(customer_state))   as state
from {{ source('raw', 'customers') }}
