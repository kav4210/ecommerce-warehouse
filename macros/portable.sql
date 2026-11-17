{# Cross-database helpers. adapter.dispatch runs the postgres__ or snowflake__ version for the active target,
   so the same models run on both warehouses. #}

{% macro date_series(start_date, end_date) -%}
    {{ return(adapter.dispatch('date_series', 'ecom_dw')(start_date, end_date)) }}
{%- endmacro %}
{% macro postgres__date_series(start_date, end_date) -%}
select d::date as d from generate_series('{{ start_date }}'::date, '{{ end_date }}'::date, interval '1 day') as g(d)
{%- endmacro %}
{% macro snowflake__date_series(start_date, end_date) -%}
{%- set n = (modules.datetime.datetime.strptime(end_date, '%Y-%m-%d') - modules.datetime.datetime.strptime(start_date, '%Y-%m-%d')).days + 1 -%}
select dateadd(day, row_number() over (order by seq4()) - 1, '{{ start_date }}'::date) as d
from table(generator(rowcount => {{ n }}))
{%- endmacro %}

{% macro iso_dow(col) -%}
    {{ return(adapter.dispatch('iso_dow', 'ecom_dw')(col)) }}
{%- endmacro %}
{% macro postgres__iso_dow(col) -%}extract(isodow from {{ col }}){%- endmacro %}
{% macro snowflake__iso_dow(col) -%}dayofweekiso({{ col }}){%- endmacro %}

{% macro seconds_between(start_ts, end_ts) -%}
    {{ return(adapter.dispatch('seconds_between', 'ecom_dw')(start_ts, end_ts)) }}
{%- endmacro %}
{% macro postgres__seconds_between(start_ts, end_ts) -%}extract(epoch from ({{ end_ts }} - {{ start_ts }})){%- endmacro %}
{% macro snowflake__seconds_between(start_ts, end_ts) -%}datediff('second', {{ start_ts }}, {{ end_ts }}){%- endmacro %}

{% macro string_agg_distinct(col) -%}
    {{ return(adapter.dispatch('string_agg_distinct', 'ecom_dw')(col)) }}
{%- endmacro %}
{% macro postgres__string_agg_distinct(col) -%}string_agg(distinct {{ col }}, ',' order by {{ col }}){%- endmacro %}
{% macro snowflake__string_agg_distinct(col) -%}listagg(distinct {{ col }}, ',') within group (order by {{ col }}){%- endmacro %}

{# Snowflake's bare ::numeric means NUMBER(38,0), which silently drops decimals, so it needs an explicit scale. #}
{% macro to_numeric(expr) -%}
    {{ return(adapter.dispatch('to_numeric', 'ecom_dw')(expr)) }}
{%- endmacro %}
{% macro postgres__to_numeric(expr) -%}({{ expr }})::numeric{%- endmacro %}
{% macro snowflake__to_numeric(expr) -%}({{ expr }})::number(38, 12){%- endmacro %}

{# Indexes and ANALYZE speed up the Postgres tests. Snowflake has neither (it prunes micro-partitions itself). #}
{% macro index_hook(cols) -%}
    {%- if target.type == 'postgres' -%}create index on {{ this }} ({{ cols }}){%- else -%}select 1{%- endif -%}
{%- endmacro %}
{% macro analyze_hook() -%}
    {%- if target.type == 'postgres' -%}analyze {{ this }}{%- else -%}select 1{%- endif -%}
{%- endmacro %}
