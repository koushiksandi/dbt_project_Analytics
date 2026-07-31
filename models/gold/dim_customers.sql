{{
    config(
        schema= 'gold',
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'customer_sk',
        merge_update_columns = [
                                    'effective_end_date',
                                    'effective_start_date',
                                    'is_current',
                                    'email',
                                    'phone',
                                    'city',
                                    'country',
                                    'state',
                                    'zip',
                                    'is_active',
                                    'loyalty_tier',
                                    'priority',
                                    'signup_date'
        ]
    )
}}

with source_data as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['full_name'])}} as scd2_hash
    from {{ ref('int_customers') }}
)

{% if is_incremental() %}
,
current_open as (
    select 
        * 
    from {{ this }}
    where is_current = true
),

change_scd2 as (
    select
        s.*,
        c.customer_sk as existing_sk,
        c.scd2_hash as existing_scd2_hash,
        c.full_name as existing_full_name,
        c.email as existing_email,
        c.phone as existing_phone,
        c.city as existing_city,
        c.country as existing_country,
        c.state as existing_state,
        c.zip as existing_zip,
        c.is_active as existing_activity,
        c.loyalty_tier as existing_loyalty_tier,
        c.priority as existing_priority,
        c.signup_date as existing_signup_date
    from source_data s
    left join current_open c
        on c.customer_id = s.customer_id
),

rows_to_close as (
    select
        existing_sk as customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        loaded_at as effective_start_date,
        current_timestamp() as effective_end_date,
        false as is_current
    from change_scd2
    where scd2_hash != existing_scd2_hash
        and existing_sk is not null
),

rows_to_open as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} as customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        loaded_at as effective_start_date,
        null::timestamp_ntz as effective_end_date,
        true as is_current
    from change_scd2
    where scd2_hash != existing_scd2_hash
        and existing_sk is not null
),

update_rows_scd1 as (
    select
        existing_sk as customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        current_timestamp() as effective_start_date,
        null::timestamp_ntz as effective_end_date,
        true as is_current
    from change_scd2
    where 
        scd2_hash = existing_scd2_hash
        and 
        existing_sk is not null
        and (
            email       is distinct from existing_email
        or phone        is distinct from existing_phone
        or city         is distinct from existing_city
        or country      is distinct from existing_country
        or state        is distinct from existing_state
        or zip          is distinct from existing_zip
        or is_active    is distinct from existing_activity
        or loyalty_tier is distinct from existing_loyalty_tier
        or priority     is distinct from existing_priority
        or signup_date  is distinct from existing_signup_date
        )
),

new_rows as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} as customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        loaded_at as effective_start_date,
        null::timestamp_ntz as effective_end_date,
        true as is_current
    from change_scd2
    where scd2_hash != existing_scd2_hash
        and existing_sk is null
),

final as (
    select
        customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from rows_to_close
    union all
    select
        customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from rows_to_open
    union all
    select
        customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from update_rows_scd1
    union all
    select
        customer_sk,
        customer_id,
        full_name,
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
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from new_rows    
) 

select 
    *
from final

{% else %}

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at'])}} as customer_sk,
    customer_id,
    full_name,
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
    scd2_hash,
    loaded_at as effective_start_date,
    null::timestamp_ntz as effective_end_date,
    true as is_current
from source_data

{% endif %}