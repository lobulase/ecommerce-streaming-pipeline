with orders as (
    select * from {{ ref('stg_orders') }}
),

daily_counts as (
    select
        date(event_timestamp) as event_date,
        event_type,
        count(*) as event_count,
        count(distinct order_id) as distinct_orders,
        round(sum(order_total), 2) as total_order_value
    from orders
    group by 1, 2
)

select * from daily_counts
order by event_date, event_type
