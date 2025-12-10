-- dim_marketplaces.sql

with

marketplaces as (

    select *
    from {{ source('public', 'amazon_marketplaces') }}
    where active = true

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['marketplace_id']) }} as marketplace_sk,
        marketplace_id,
        marketplace_name,
        region,
        endpoint

    from marketplaces

)

select * from add_surrogate_key
