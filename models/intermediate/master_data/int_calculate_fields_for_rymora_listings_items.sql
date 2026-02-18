-- int_calculate_fields_for_rymora_listings_items.sql

{{ config(materialized='view') }}

with

filtered_ry_listings as (

    select * from {{ ref('int_filter_rymora_listings_items') }}

),

add_fields as (

    select
        *,

        -- Parent Code
        case
            when REGEXP_CONTAINS(sku, r"^R_COMP-SOCKS")
                then "R_COMP-SOCKS"
            else CONCAT(TRIM(SPLIT(sku, "_")[SAFE_OFFSET(0)]), "_", TRIM(SPLIT(sku, "_")[SAFE_OFFSET(1)]))
        end as parent_code,

        -- Portfolio Code
        case
            when REGEXP_CONTAINS(sku, r"^R_COMP-SOCKS")
                then "R_COMP-SOCKS"
            else CONCAT(TRIM(SPLIT(sku, "_")[SAFE_OFFSET(0)]), "_", TRIM(SPLIT(sku, "_")[SAFE_OFFSET(1)]))
        end as portfolio_code,

        -- Product Code
        SPLIT(ARRAY_REVERSE(SPLIT(sku, "_"))[OFFSET(0)], "-")[SAFE_OFFSET(0)] as product_code,

        -- Product Color Code
        case
            when not REGEXP_CONTAINS(sku, r"^R_TT-SET")
                then TRIM(SPLIT(sku, "_")[SAFE_OFFSET(2)])
        end as product_color_code,

        -- Product Pack Size
        case
            when REGEXP_CONTAINS(sku, r"R_COMP-SOCKS")    -- hard-coded because it's not in the SKU
                then "1PR"
            when not REGEXP_CONTAINS(sku, r"^R_TT-SET")
                then TRIM(SPLIT(sku, "_")[SAFE_OFFSET(3)])
        end as product_pack_size

    from filtered_ry_listings

),

standardize_product_color as (

    select
        sku,
        asin,
        product_type,
        tenant_id,
        parent_code,
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
                then
                    case
                        when REGEXP_CONTAINS(sku, r"^R_CALF-SLEEV")
                            then "ROYAL BLUE"
                        else "BLUE"
                    end
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
                then "NEON YELLOW"
            when product_color_code = "GRN"
                then "GREEN"
            when product_color_code = "GRY"
                then "GRAY"
            when product_color_code = "MNT"
                then "MINT"
            when product_color_code = "NA-GR"
                then "NAVY-GRAY"
            when product_color_code = "NDE"
                then "LIGHT NUDE"
            when product_color_code = "OLV"
                then "OLIVE GREEN"
            when product_color_code = "ORG"
                then "ORANGE"
            when product_color_code = "PIN"
                then
                    case
                        when REGEXP_CONTAINS(sku, r"^R_CALF-SLEEV")
                            then "HOT PINK"
                        else "PINK"
                    end
            when product_color_code = "PU-TE"
                then "PURPLE-TEAL"
            when product_color_code = "PUR"
                then
                    case
                        when REGEXP_CONTAINS(sku, r"^R_CALF-SLEEV")
                            then "ROYAL VIOLET"
                        else "PURPLE"
                    end
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
        portfolio_code,
        product_code,
        product_color_code,
        product_color,
        product_pack_size

    from standardize_product_color

),

-- remove duplicate ASINs
remove_skus_with_uln_and_sln as (   -- not present in SKU Mastersheet (17 rows)

    select *
    from reorder_fields
    where not REGEXP_CONTAINS(sku, r"_(U-LN|U_LN|-U-LN|S-LN)$")

),

remove_product_code_ltm2f as (  -- not present in SKU Mastersheet (1 row)

    select *
    from remove_skus_with_uln_and_sln
    where not REGEXP_CONTAINS(sku, r"_LTm2F$")

),

remove_old_sku as ( -- old SKU of R_COMP-SOCKS-PL_BLK_V4_SL_SXm4

    select *
    from remove_product_code_ltm2f
    where not REGEXP_CONTAINS(sku, r"R_COMP-SOCKS-PL_BLK_V4_SL_SXm4-")

)

select * from remove_old_sku
