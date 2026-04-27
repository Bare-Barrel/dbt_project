-- int_remove_rows_from_bb_listings_items.sql md_bb_3

{{ config(materialized='view') }}

with

bb_listings_with_calc_fields as (

    select * from {{ ref('int_calculate_fields_for_bb_listings_items') }}

),

filter_nulls as (

    select *
    from bb_listings_with_calc_fields
    where shaker_code is not null

),

remove_mainparisan as (     -- test code only acc to Savan

    select *
    from filter_nulls
    where not REGEXP_CONTAINS(sku, r"MainParisan")

)

select * from remove_mainparisan
