{{ config(severity='warn') }}
select *
from {{ ref('fct_orders') }}
where is_confirmed = true and paid_amount > order_amount