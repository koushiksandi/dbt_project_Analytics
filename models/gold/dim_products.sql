{{
    config(
        schema= 'gold',
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'product_sk',
        merge_update_columns = [
                                    'effective_end_date', 
                                    'is_current',
                                    'rating',
                                    'tags',
                                    'effective_start_date'
        ]
    )
}}

with source_data as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['category', 'stock_status', 'name', 'price']) }} as scd2_hash
    from {{ ref('int_products') }}
)

{% if is_incremental() %}

,
current_open as (
    select
        *
    from {{ this }}
    where is_current = true
),

change_detection as (
    select
        s.*,
        c.product_sk    as existing_surrogate_key,
        c.category      as existing_category,
        c.stock_status  as existing_stock_status,
        c.name          as existing_name,
        c.price         as existing_price,
        c.rating        as existing_rating,
        c.tags          as existing_tags,
        c.scd2_hash     as existing_scd2_hash
    from source_data s
    left join current_open c
        on s.product_id = c.product_id
),

deletion_detection as (
    select
        s.*,
        c.product_sk    as existing_surrogate_key,
        c.category      as existing_category,
        c.stock_status  as existing_stock_status,
        c.name          as existing_name,
        c.price         as existing_price,
        c.rating        as existing_rating,
        c.tags          as existing_tags,
        c.scd2_hash     as existing_scd2_hash
    from source_data s
    right join current_open c
        on s.product_id = c.product_id
    where s.product_id is null
),

rows_to_add_new as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'updated_at'])}} as product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        loaded_at as effective_start_date,
        null::timestamp_ntz as effective_end_date,
        true as is_current
    from change_detection
    where existing_surrogate_key is null
),

rows_to_open as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'updated_at'])}} 
                                    as product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        updated_at                  as effective_start_date,
        null::timestamp_ntz         as effective_end_date,
        true                        as is_current
    from change_detection
    where existing_surrogate_key    is not null 
        and scd2_hash               is distinct from existing_scd2_hash
),

rows_to_close as (
    select
        existing_surrogate_key      as product_sk,
        product_id                  as product_id,
        existing_category           as category,
        existing_stock_status       as stock_status,
        existing_name               as name,
        existing_price              as price,
        existing_rating             as rating,
        existing_tags               as tags,
        existing_scd2_hash          as scd2_hash,
        loaded_at                   as effective_start_date,
        current_timestamp()         as effective_end_date,
        false                       as is_current
    from change_detection
    where existing_surrogate_key    is not null 
        and scd2_hash               is distinct from existing_scd2_hash
),

rows_to_update_scd1 as (
    select
        existing_surrogate_key      as product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        updated_at                  as effective_start_date,
        null::timestamp_ntz         as effective_end_date,
        true                        as is_current
    from change_detection
    where existing_surrogate_key    is not null 
        and scd2_hash               = existing_scd2_hash
),

rows_deleted_at_source as (
    select
        existing_surrogate_key      as product_sk,
        existing_product_id         as product_id,
        existing_category           as category,
        existing_stock_status       as stock_status,
        existing_name               as name,
        existing_price              as price,
        existing_rating             as rating,
        existing_tags               as tags,
        existing_scd2_hash          as scd2_hash,
        effective_start_date,
        current_timestamp()         as effective_end_date,
        false                       as is_current
    from deletion_detection
    where existing_surrogate_key    is not null
), 

final as (
    select
        product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from rows_to_add_new
    union all
    select
        product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from rows_to_close
    union all
    select
        product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from rows_to_open
    union all
    select
        product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from rows_to_update_scd1
    union all
    select
        product_sk,
        product_id,
        category,
        stock_status,
        name,
        price,
        rating,
        tags,
        scd2_hash,
        effective_start_date,
        effective_end_date,
        is_current
    from rows_deleted_at_source
)
select
    *
from final

{% else %}

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'updated_at'])}} 
                                as product_sk,
    product_id,
    category,
    stock_status,
    name,
    price,
    rating,
    tags,
    scd2_hash,
    updated_at                  as effective_start_date,
    null::timestamp_ntz         as effective_end_date,
    true                        as is_current
from source_data

{% endif %}


