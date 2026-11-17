{# Olist has several reviews per order. Keep ONE per order (latest answered, tie-break review_id) so the order-grain fact never fans out. #}
with ranked as (
    select
        order_id,
        review_id,
        review_score::int                              as review_score,
        nullif(review_creation_date, '')::timestamp    as created_ts,
        nullif(review_answer_timestamp, '')::timestamp as answered_ts,
        row_number() over (
            partition by order_id
            order by nullif(review_answer_timestamp, '')::timestamp desc nulls last, review_id desc
        ) as rn
    from {{ source('raw', 'order_reviews') }}
)
select order_id, review_id, review_score, created_ts, answered_ts
from ranked
where rn = 1
