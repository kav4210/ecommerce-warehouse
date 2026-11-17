-- every version has positive width and starts exactly where the previous one ends (no gap, no overlap)
with v as (
    select customer_unique_id, valid_from, valid_to,
           lag(valid_to) over (partition by customer_unique_id order by valid_from) as prev_to
    from {{ ref('dim_customer') }} where customer_key <> 'unknown'
)
select * from v where valid_from >= valid_to or (prev_to is not null and prev_to <> valid_from)
