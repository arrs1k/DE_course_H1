{{ config(
    materialized='table',
    distributed_by='order_id',
    orientation='column'
) }}

with order_items as (
    select * from {{ ref('stg_shop__order_items') }}
),

orders as (
    select * from {{ ref('stg_shop__orders') }}
),

final as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,
        o.order_date,
        oi.quantity,
        oi.unit_price,
        oi.discount_amount,
        oi.line_amount,
        o.order_status = 'paid' as is_confirmed
    from order_items oi
    left join orders o 
        on oi.order_id = o.order_id
    where not o.is_test_order
)

select * from final