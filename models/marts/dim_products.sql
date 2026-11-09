{{ config(
    materialized='table',
    distributed_replicated=true
) }}

with products as (
    select 
        p.product_id,
        p.product_name,
        p.brand,
        c.category_name,
        c.parent_category_name,
        c.root_category_name
    from {{ ref('stg_shop__products') }} p
    left join {{ ref('int_product_categories_flat') }} c 
        on p.category_name = c.category_name
)

select * from products