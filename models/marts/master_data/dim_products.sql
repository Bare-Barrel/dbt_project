-- dim_products.sql

with

bb_product_codes as (

    select
        *,
        CAST(null as string) as product_color_code,
        CAST(null as string) as product_color,
        CAST(null as string) as product_pack_size
    from {{ ref('int_calculate_fields_for_bb_listings_items') }}

),

rymora_product_codes as (

    select
        *,
        CAST(null as string) as shaker_code
    from {{ ref('int_calculate_fields_for_rymora_listings_items') }}

),

reorder_rymora_fields as (

    select
        sku,
        asin,
        product_type,
        tenant_id,
        parent_code,
        shaker_code,
        portfolio_code,
        product_code,
        product_color_code,
        product_color,
        product_pack_size

    from rymora_product_codes

),

union_all as (

    select * from bb_product_codes

    union all

    select * from reorder_rymora_fields

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['asin']) }} as product_sk,
        *

    from union_all

)

select * from add_surrogate_key
