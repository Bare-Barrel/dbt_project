-- dim_portfolios.sql

with

portfolios as (

    select * from {{ ref('stg_public__amazon_advertising_portfolios') }}

),

select_fields as (

    select
        portfolio_id,
        name as portfolio_name

    from portfolios

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
