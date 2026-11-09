{{ config(
    materialized='table',
    distributed_replicated=true
) }}

with customers as (
    select * from {{ ref('stg_shop__customers') }}
),

confirmed_orders as (
    select 
        customer_id,
        count(distinct order_id) as confirmed_orders_count,
        sum(order_amount) as confirmed_revenue
    from {{ ref('fct_orders') }}
    where is_confirmed = true
    group by customer_id
),

first_orders as (
    select 
        customer_id,
        order_id as first_confirmed_order_id,
        ordered_at as first_confirmed_at,
        row_number() over (partition by customer_id order by ordered_at asc) as rn
    from {{ ref('fct_orders') }}
    where is_confirmed = true
)

select
    c.customer_id,
    c.registration_date,
    c.city,
    c.acquisition_channel,
    f.first_confirmed_order_id,
    f.first_confirmed_at,
    coalesce(co.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(co.confirmed_revenue, 0) as confirmed_revenue,
    case 
        when coalesce(co.confirmed_orders_count, 0) > 0 then true 
        else false 
    end as is_buyer
from customers c
left join first_orders f 
    on c.customer_id = f.customer_id and f.rn = 1
left join confirmed_orders co 
    on c.customer_id = co.customer_id