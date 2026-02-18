-- int_calculate_fields_for_bb_listings_items.sql   TODO: get product colors from google sheets MD

{{ config(materialized='view') }}

with

filtered_bb_listings as (

    select * from {{ ref('int_filter_bb_listings_items') }}

),

extract_fields as (

    select
        *,
        SPLIT(TRIM(SPLIT(sku, "_")[SAFE_OFFSET(1)]), "-")[SAFE_OFFSET(0)] as parent_code,
        SPLIT(TRIM(SPLIT(sku, "_")[SAFE_OFFSET(1)]), "-")[SAFE_OFFSET(1)] as shaker_code,
        REGEXP_REPLACE(
            SPLIT(ARRAY_REVERSE(SPLIT(sku, "_"))[OFFSET(0)], "-")[SAFE_OFFSET(0)],
            r"[^A-Za-z]", ""
        ) as product_code

    from filtered_bb_listings

),

add_concat_field as (

    select
        sku,
        asin,
        product_type,
        tenant_id,
        parent_code,
        shaker_code,
        CONCAT(parent_code, "-", shaker_code) as portfolio_code,
        product_code

    from extract_fields

),

filter_nulls as (

    select *
    from add_concat_field
    where shaker_code is not null

),

remove_mainparisan as (     -- test code only acc to Savan

    select *
    from filter_nulls
    where not REGEXP_CONTAINS(sku, r"MainParisan")

)

select * from remove_mainparisan
