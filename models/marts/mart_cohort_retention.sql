{# Cohort = month of a person's first non-canceled order. Retained = ordered again in month N after it. #}
with orders as (
    select c.customer_unique_id, date_trunc('month', f.purchase_ts)::date as m
    from {{ ref('fct_orders') }} f
    join {{ ref('dim_customer') }} c on c.customer_key = f.customer_key
    where f.status not in ('canceled', 'unavailable')
),
first as (select customer_unique_id, min(m) as cohort_month from orders group by 1),
act as (
    select f.cohort_month,
           (extract(year from age(o.m, f.cohort_month)) * 12 + extract(month from age(o.m, f.cohort_month)))::int as month_offset,
           count(distinct o.customer_unique_id) as active
    from orders o join first f using (customer_unique_id)
    group by 1, 2
)
select a.cohort_month, a.month_offset, a.active,
       s.active as cohort_size,
       round(100.0 * a.active / s.active, 2) as retention_pct
from act a
join act s on s.cohort_month = a.cohort_month and s.month_offset = 0
