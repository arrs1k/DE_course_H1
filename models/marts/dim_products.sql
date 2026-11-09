{{ config(
    materialized='table',
    distributed_replicated=true
) }}

with recursive category_tree as (
    select 
        category_id,
        category_name,
        parent_category_id,
        category_name as root_category_name,
        1 as level
    from {{ ref('stg_shop__product_categories') }}
    where parent_category_id is null

    union all

    select 
        c.category_id,
        c.category_name,
        c.parent_category_id,
        ct.root_category_name,
        ct.level + 1
    from {{ ref('stg_shop__product_categories') }} c
    join category_tree ct on c.parent_category_id = ct.category_id
),

categories_flat as (
    select 
        ct.category_id,
        ct.category_name,
        pc.category_name as parent_category_name,
        ct.root_category_name
    from category_tree ct
    left join {{ ref('stg_shop__product_categories') }} pc 
        on ct.parent_category_id = pc.category_id
),

products as (
    select 
        p.product_id,
        p.product_name,
        p.brand,
        c.category_name,
        c.parent_category_name,
        c.root_category_name
    from {{ ref('stg_shop__products') }} p
    left join categories_flat c 
        on p.category_name = c.category_name
)

select * from products