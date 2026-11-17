with g as (
    {{ date_series('2016-01-01', '2018-12-31') }}
)
select
    to_char(d, 'YYYYMMDD')::int          as date_key,
    d::date                              as date_day,
    extract(year from d)::int            as year,
    extract(quarter from d)::int         as quarter,
    extract(month from d)::int           as month,
    to_char(d, 'YYYY-MM')                as year_month,
    {{ iso_dow('d') }}::int              as iso_dow,
    {{ iso_dow('d') }} in (6, 7)         as is_weekend
from g
