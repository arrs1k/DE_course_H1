{{ config(
    materialized='table',
    distributed_replicated=true
) }}

with date_spine as (
    select 
        generate_series(
            (select min(order_date) from {{ ref('fct_orders') }}),
            (select max(order_date) from {{ ref('fct_orders') }}),
            interval '1 day'
        )::date as date_day
)

select
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    to_char(date_day, 'Month') as month_name,
    extract(day from date_day) as day_of_month,
    extract(isodow from date_day) as day_of_week,
    to_char(date_day, 'Day') as day_name,
    case 
        when extract(isodow from date_day) in (6, 7) then true 
        else false 
    end as is_weekend
from date_spine