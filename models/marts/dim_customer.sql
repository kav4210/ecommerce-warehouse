{{ config(post_hook=[
    "create index on {{ this }} (customer_key)",
    "create index on {{ this }} (customer_unique_id, valid_from)",
    "analyze {{ this }}"
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
           lag(city)  over w as prev_city,
           lag(state) over w as prev_state
    from o
    window w as (partition by customer_unique_id order by purchase_ts, order_id)
),
runs as (
    select *,
           sum(case when prev_city is null or city <> prev_city or state <> prev_state then 1 else 0 end)
               over (partition by customer_unique_id order by purchase_ts, order_id
                     rows between unbounded preceding and current row) as run_id
    from marked
),
versions as (
    select
        customer_unique_id,
        run_id,
        min(purchase_ts)                                          as valid_from,
        (array_agg(city  order by purchase_ts, order_id))[1]      as city,
        (array_agg(state order by purchase_ts, order_id))[1]      as state
    from runs
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
