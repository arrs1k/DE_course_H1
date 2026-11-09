with source_payments as (
    select order_id
    from {{ source('shop', 'payments') }}
    where payment_status = 'success' and amount > 0
    group by order_id
),
source_revenue as (
    select sum(o.total_amount) as total_source
    from {{ source('shop', 'orders') }} o
    join source_payments p on o.order_id = p.order_id
    where o.status = 'paid'
),
marts_revenue as (
    select sum(order_amount) as total_marts
    from {{ ref('fct_orders') }}
    where is_confirmed = true
)
select * from source_revenue, marts_revenue
where source_revenue.total_source != marts_revenue.total_marts