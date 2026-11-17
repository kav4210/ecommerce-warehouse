-- fact items revenue must equal raw staging revenue to the cent
select f.fct, s.src
from (select sum(price) fct from {{ ref('fct_order_items') }}) f,
     (select sum(price) src from {{ ref('stg_order_items') }}) s
where f.fct <> s.src
