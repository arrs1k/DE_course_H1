with source as (
    select * from {{ source('shop', 'product_categories') }}
)

select
    category_id,
    category_name,
    parent_category_id
from source