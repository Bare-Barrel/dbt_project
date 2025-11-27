-- int_filter_rymora_listings_items.sql

{{ config(materialized='ephemeral') }}

with

listings_summaries as (

    select * from {{ source('listings_items', 'summaries') }}

),

latest_date as (

    select max(date) as max_date
    from listings_summaries
    where tenant_id = 1

),

get_rows_with_max_dates as (

    select
        sku,
        asin,
        product_type,
        tenant_id
    from listings_summaries as ls
    where
        ls.tenant_id = 2
        and ls.date = (select ld.max_date from latest_date as ld)

),

unique_rows as (

    select distinct *
    from get_rows_with_max_dates

),

get_rymora_skus as (

    select *
    from unique_rows
    where regexp_contains(sku, r"^R_.*")

)

select * from get_rymora_skus
