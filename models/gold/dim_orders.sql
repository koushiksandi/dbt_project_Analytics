{{
    config(
        schema= 'gold'
    )
}}
with dim_orders as (
    select * from {{ ref('orders_snapshot_gold') }}
)
select
    customer_id,
    order_id,
    order_date,
    order_total,
    payment_method,
    line_item_id,
    line_total,
    product_id,
    quantity,
    unit_price,
    status,
    dbt_valid_from as effective_start_date,
    dbt_valid_to as effective_end_date,
    case
        when dbt_valid_to is null then 'Current details'
        else 'Expired details'
    end as Expiry
from dim_orders