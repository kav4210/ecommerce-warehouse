-- joins to items, payments, reviews, SCD2 must not change the order count
select f.n_fact, s.n_src
from (select count(*) n_fact from {{ ref('fct_orders') }}) f,
     (select count(*) n_src from {{ ref('stg_orders') }}) s
where f.n_fact <> s.n_src
