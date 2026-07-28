{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= 'line_item_id',
        schema= 'silver'
    )
}}
WITH int_orders as (
    select * from {{ source('analytics', 'orders_flat') }}
)
select
    customer_id,
    order_id,
    order_date::timestamp_ntz as order_date,
    order_total::number(15, 2) as order_total,
    payment_method,
    concat(address:city::string, ', ', address:state::string, ', ', address:country::string, ', ', address:zip::string) as address,
    line_item_id,
    line_total::number(10, 2) as line_total,
    product_id,
    product_name,
    quantity::int as quantity,
    unit_price::number(10, 2) as unit_price,
    case
        when status in ('returned', 'delivered') then 'closed'
        when status = 'cancelled' then 'cancelled'
        else 'open'
    end as status,
    loaded_at,
    updated_at
from int_orders

{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }} )
{% endif %}