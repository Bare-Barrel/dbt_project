-- dim_products.sql

with

bb_product_codes as (

    select * from {{ ref('int_calculate_fields_for_bb_listings_items') }}

),

rymora_product_codes as (

    select * from {{ ref('int_calculate_fields_for_rymora_listings_items') }}

),

union_all as (

    select
        *,
        CAST(null as string) as product_color_code,
        CAST(null as string) as product_color,
        CAST(null as string) as product_pack_size
    from bb_product_codes

    union all

    select * from rymora_product_codes

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['asin']) }} as product_sk,
        *

    from union_all

)

select * from add_surrogate_key
