{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= 'customer_id',
        schema= 'silver'
    )
}}
WITH int_customers as (
    select * from {{ source('analytics', 'customers_flat') }}
)
select
    customer_id,
    trim(concat(first_name, ' ', last_name)) as full_name,
    email,
    phone,
    address:city::string as city,
    address:country::string as country,
    address:state::string as state,
    address:zip::string as zip,
    is_active::boolean as is_active,
    loyalty_tier,
    case
        when is_active = true and loyalty_tier in ('platinum', 'gold') then 'high'
        when is_active = true and loyalty_tier in ('silver', 'bronze') then 'medium'
        when is_active = false then 'low'
    end as priority,
    signup_Date::timestamp_ntz as signup_date,
    loaded_at,
    current_timestamp() as updated_at
from int_customers

{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}