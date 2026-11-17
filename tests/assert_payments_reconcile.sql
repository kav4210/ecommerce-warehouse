select f.fct, s.src
from (select sum(payment_value) fct from {{ ref('fct_orders') }}) f,
     (select sum(payment_value) src from {{ ref('stg_payments') }}) s
where f.fct <> s.src
