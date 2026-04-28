-- dim_portfolios.sql

with

portfolios_v3 as (

    select * from {{ ref('stg_amazon_ads_api__amazon_advertising_portfolios_v3') }}

),

select_fields as (

    select
        portfolio_id,
        portfolio_name

    from portfolios_v3

),

get_unique_portfolio_values as (

    select distinct *
    from select_fields

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['portfolio_id']) }} as portfolio_sk,
        portfolio_id,
        portfolio_name

    from get_unique_portfolio_values

)

select * from add_surrogate_key
