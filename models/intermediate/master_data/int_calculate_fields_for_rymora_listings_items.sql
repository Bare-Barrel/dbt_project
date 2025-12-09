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
        CONCAT(TRIM(SPLIT(sku, "_")[SAFE_OFFSET(0)]), "_", TRIM(SPLIT(sku, "_")[SAFE_OFFSET(1)])) as portfolio_code,
        SPLIT(ARRAY_REVERSE(SPLIT(sku, "_"))[OFFSET(0)], "-")[SAFE_OFFSET(0)] as product_code,
        TRIM(SPLIT(sku, "_")[SAFE_OFFSET(2)]) as product_color_code,
        TRIM(SPLIT(sku, "_")[SAFE_OFFSET(3)]) as product_pack_size

    from filtered_ry_listings

),

standardize_product_color as (

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
        product_pack_size,

        -- Product Color
        case
            when product_color_code = "BEI"
                then "WARM BEIGE"
            when product_color_code = "BK-BK"
                then "BLACK-BLACK"
            when product_color_code = "BLK"
                then "BLACK"
            when product_color_code = "BLU"
                then "BLUE"
            when product_color_code = "BRG"
                then "BURGUNDY"
            when product_color_code = "BRN"
                then "BROWN"
            when product_color_code = "BRN-GRN-GRY"
                then "MIX"
            when product_color_code = "CHR"
                then "CHARCOAL"
            when product_color_code = "COA"
                then "DEEP COCOA"
            when product_color_code = "FLU"
                then "NEON YELLOW/FLUORESCENT"
            when product_color_code = "GRN"
                then "GREEN"
            when product_color_code = "GRY"
                then "GREY"
            when product_color_code = "MNT"
                then "MINT"
            when product_color_code = "NA-GR"
                then "NAVY-GREY"
            when product_color_code = "NDE"
                then "LIGHT NUDE"
            when product_color_code = "OLV"
                then "OLIVE GREEN"
            when product_color_code = "ORG"
                then "ORANGE"
            when product_color_code = "PIN"
                then "PINK"
            when product_color_code = "PU-TE"
                then "PURPLE-TEAL"
            when product_color_code = "PUR"
                then "PURPLE"
            when product_color_code = "RED"
                then "FIERY RED"
            when product_color_code = "RSE"
                then "ROSE CORAL"
            when product_color_code = "SKY"
                then "SKY BLUE"
            when product_color_code = "TRQ"
                then "TURQUOISE"
            when product_color_code = "WHT"
                then "WHITE"
        end as product_color

    from add_fields

),

reorder_fields as (

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

    from standardize_product_color

),

-- remove duplicate ASINs
remove_product_code_u as (

    select *
    from reorder_fields
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
