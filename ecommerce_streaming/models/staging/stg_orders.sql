with source as (
    select * from {{ source('ecommerce_streaming', 'orders_raw') }}
),

cleaned as (
    select
        order_id,
        event_type,
        timestamp(event_timestamp) as event_timestamp,
        customer_id,
        product_id,
        product_category,
        quantity,
        unit_price,
        order_total,
        payment_method,
        shipping_country,
        timestamp(ingested_at) as ingested_at
    from source
    where order_id is not null
      and event_type in ('order_placed', 'payment_confirmed', 'shipped', 'delivered')
)

select * from cleaned
