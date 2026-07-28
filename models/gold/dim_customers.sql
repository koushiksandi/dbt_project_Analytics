{{
    config(
        schema= 'gold',
        
    )
}}
with dim_customers as (
    select * from {{ ref('customers_snapshot_gold') }}
)
select
    customer_id,
    full_name
    email,
    phone,
    city,
    country,
    state,
    zip,
    is_active,
    loyalty_tier,
    priority,
    signup_Date,
    tenure,
    dbt_valid_from as effective_start_date,
    dbt_valid_to as effective_end_date,
    case
        when dbt_valid_to is null then 'Current details'
        else 'Expired details'
    end as Expiry
from dim_customers