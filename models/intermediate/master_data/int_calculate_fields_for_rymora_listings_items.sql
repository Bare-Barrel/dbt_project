-- int_calculate_fields_for_rymora_listings_items.sql

{{ config(materialized='view') }}

with

filtered_ry_listings as (

    select * from {{ ref('int_filter_rymora_listings_items') }}

),

add_fields as (

    select
        *,
        CONCAT(TRIM(SPLIT(sku, "_")[SAFE_OFFSET(0)]), "_", TRIM(SPLIT(sku, "_")[SAFE_OFFSET(1)])) as parent_code,
        CONCAT(TRIM(SPLIT(sku, "_")[SAFE_OFFSET(2)]), "_", TRIM(SPLIT(sku, "_")[SAFE_OFFSET(3)])) as shaker_code,
        SPLIT(ARRAY_REVERSE(SPLIT(sku, "_"))[OFFSET(0)], "-")[SAFE_OFFSET(0)] as product_code
    from filtered_ry_listings

),

add_concat_field as (

    select
        sku,
        asin,
        product_type,
        tenant_id,
        parent_code,
        shaker_code,
        parent_code as portfolio_code,
        product_code
    from add_fields

),

-- remove duplicate ASINs
remove_product_code_u as (

    select *
    from add_concat_field
    where product_code is distinct from "U"

),

remove_product_code_s as (

    select *
    from remove_product_code_u
    where product_code is distinct from "S"

),

remove_sku_last_dash as (

    select *
    from remove_product_code_s
    where sku is distinct from "R_COMP-SOCKS-PL_BLK_V4_SL_SXm4-"

),

remove_product_code_ltm2f as (

    select *
    from remove_sku_last_dash
    where product_code is distinct from "LTm2F"

)

select * from remove_product_code_ltm2f
