{{
    config(
        schema= 'gold'
    )
}}
with dim_products as (
    select * from {{ ref('products_snapshot_gold') }}
)
select
    product_id,
    category,
    stock_status,
    price,
    rating,
    tags,
    dbt_valid_from as effective_start_date,
    dbt_valid_to as effective_end_date,
    case
        when dbt_valid_to is null then 'Current details'
        else 'Expired details'
    end as Expiry
from dim_products