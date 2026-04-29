-- stg_public__amazon_advertising_portfolios.sql

with

amazon_advertising_portfolios as (

    select * from {{ source('public', 'amazon_advertising_portfolios') }}

),

rename_fields as (

    select
        creation_date,
        last_updated_date,
        created_at,
        updated_at,
        portfolio_id,
        tenant_id,
        name as portfolio_name,
        state as portfolio_state,
        marketplace,
        in_budget,
        serving_status

    from amazon_advertising_portfolios

),

cast_data_types as (

    select
        -- ids
        CAST(portfolio_id as string) as portfolio_id,
        tenant_id,

        -- strings
        portfolio_name,
        portfolio_state,
        marketplace,
        serving_status,

        --boolean
        in_budget,

        -- datetime
        creation_date,
        last_updated_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
