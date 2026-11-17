-- the version an order is joined to must carry the exact city+state that order was placed from
select o.order_id, c.city as src_city, c.state as src_state, d.city as dim_city, d.state as dim_state
from {{ ref('fct_orders') }} o
join {{ ref('stg_orders') }} so using (order_id)
join {{ ref('stg_customers') }} c on c.customer_id = so.customer_id
join {{ ref('dim_customer') }} d on d.customer_key = o.customer_key
where d.city <> c.city or d.state <> c.state
