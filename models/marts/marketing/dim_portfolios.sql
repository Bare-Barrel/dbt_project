-- dim_portfolios.sql

with

portfolios as (

    select * from {{ source('public', 'amazon_advertising_portfolios') }}

),

dim_tenants as (

    select * from {{ ref('dim_tenants') }}

),

select_fields as (

    select
        portfolio_id,
        name as portfolio_name,
        tenant_id

    from portfolios

),

get_unique_values as (

    select distinct *
    from select_fields

),

get_tenant_sk as (

    select
        guv.portfolio_id,
        guv.portfolio_name,
        dt.tenant_sk

    from get_unique_values as guv

    left join dim_tenants as dt
        on guv.tenant_id = dt.tenant_id

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['portfolio_id', 'tenant_sk']) }} as portfolio_sk,
        portfolio_id,
        portfolio_name,
        tenant_sk

    from get_tenant_sk

)

select * from add_surrogate_key
