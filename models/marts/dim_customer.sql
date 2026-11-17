{{ config(post_hook=[
    "{{ index_hook('customer_key') }}",
    "{{ index_hook('customer_unique_id, valid_from') }}",
    "{{ analyze_hook() }}"
]) }}
{# SCD Type 2 derived from REAL order history.
   Olist gives every order its own customer_id, but customer_unique_id is the person. When one person's
   orders show a different city/state over time, that is an observed location change. Each consecutive run of
   the same city+state becomes one version, validity half-open [valid_from, valid_to). #}
with o as (
    select c.customer_unique_id, o.order_id, o.purchase_ts, c.city, c.state
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_customers') }} c using (customer_id)
),
marked as (
    select *,
           lag(city)  over (partition by customer_unique_id order by purchase_ts, order_id) as prev_city,
           lag(state) over (partition by customer_unique_id order by purchase_ts, order_id) as prev_state
    from o
),
runs as (
    select *,
           sum(case when prev_city is null or city <> prev_city or state <> prev_state then 1 else 0 end)
               over (partition by customer_unique_id order by purchase_ts, order_id
                     rows between unbounded preceding and current row) as run_id
    from marked
),
numbered as (
    select *,
           row_number() over (partition by customer_unique_id, run_id order by purchase_ts, order_id) as rn
    from runs
),
versions as (
    select
        customer_unique_id,
        run_id,
        min(purchase_ts)                          as valid_from,
        max(case when rn = 1 then city  end)      as city,
        max(case when rn = 1 then state end)      as state
    from numbered
    group by customer_unique_id, run_id
)
select
    md5(customer_unique_id || '|' || run_id::text)                as customer_key,
    customer_unique_id,
    city,
    state,
    valid_from,
    coalesce(lead(valid_from) over (partition by customer_unique_id order by valid_from, run_id),
             '9999-12-31'::timestamp)                             as valid_to,
    lead(valid_from) over (partition by customer_unique_id order by valid_from, run_id) is null as is_current
from versions
union all
select 'unknown', 'unknown', 'unknown', 'unknown', '1900-01-01'::timestamp, '9999-12-31'::timestamp, true
