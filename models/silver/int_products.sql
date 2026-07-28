{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= 'product_id',
        schema= 'silver'
    )
}}
WITH int_products as (
    select * from {{ source('analytics', 'products_flat') }}
)
select
    product_id,
    category,
    in_stock::boolean as stock_status,
    name,
    price::number(10, 2) as price,
    rating::number(2, 1) as rating,
    nullif(array_to_string(tags, ', '), '') as tags,
    loaded_at,
    updated_at
from int_products

{% if is_incremental() %}
where updated_at > ( select max(updated_at) from {{ this }} )
{% endif %}