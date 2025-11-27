-- int_filter_bb_listings_items.sql

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

filtered as (

    select
        sku,
        asin,
        product_type,
        tenant_id
    from listings_summaries as ls
    where
        ls.tenant_id = 1
        and ls.date = (select ld.max_date from latest_date as ld)

),

unique_rows as (

    select distinct *
    from filtered

),

ranked as (

    select
        *,
        row_number() over (partition by asin order by sku) as rn
    from unique_rows

),

remove_duplicate_asin as (

    select
        sku,
        asin,
        product_type,
        tenant_id
    from ranked
    where rn = 1

)

select * from remove_duplicate_asin
