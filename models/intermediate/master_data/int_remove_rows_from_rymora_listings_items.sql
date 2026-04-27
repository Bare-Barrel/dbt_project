-- int_remove_rows_from_rymora_listings_items.sql md_ry_3

{{ config(materialized='view') }}

with

ry_listings_with_calc_fields as (

    select * from {{ ref('int_calculate_fields_for_rymora_listings_items') }}

),

-- remove duplicate ASINs
remove_skus_with_uln_and_sln as (   -- not present in SKU Mastersheet (17 rows)

    select *
    from ry_listings_with_calc_fields
    where not REGEXP_CONTAINS(sku, r"_(U-LN|U_LN|-U-LN|S-LN)$")

),

remove_product_code_ltm2f as (  -- not present in SKU Mastersheet (1 row)

    select *
    from remove_skus_with_uln_and_sln
    where not REGEXP_CONTAINS(sku, r"_LTm2F$")

),

remove_product_code_fn as (     -- not present in SKU Mastersheet (1 row)

    select *
    from remove_product_code_ltm2f
    where not REGEXP_CONTAINS(sku, r"_FN|-FN$")
),

remove_old_sku as ( -- old SKU of R_COMP-SOCKS-PL_BLK_V4_SL_SXm4

    select *
    from remove_product_code_fn
    where not REGEXP_CONTAINS(sku, r"R_COMP-SOCKS-PL_BLK_V4_SL_SXm4-")

)

select * from remove_old_sku
