-- stg_public__amazon_marketplaces.sql

with

amazon_marketplaces as (

    select * from {{ source('public','amazon_marketplaces') }}

),

rename_fields as (

    select
        active,
        region,
        endpoint as marketplace_endpoint_url,
        marketplace_id,
        marketplace_name

    from amazon_marketplaces

)

select * from rename_fields
