-- dim_product_codes.sql

with

bb_product_codes as (

    select * from {{ ref('int_calculate_fields_for_bb_listings_items') }}

),

rymora_product_codes as (

    select * from {{ ref('int_calculate_fields_for_rymora_listings_items') }}

),

union_all as (

    select * from bb_product_codes

    union all

    select * from rymora_product_codes

)

select * from union_all
