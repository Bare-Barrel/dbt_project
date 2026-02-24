-- int_kpi_tracker__get_sb_cc_portfolio_code.sql kpi_sb_cc_01
-- Advertising reports from advertising console (2022-07-10 - 2023-06-10)

{{ config(materialized='ephemeral') }}

with

sb_campaign_console as (

    select * from {{ ref('stg_sponsored_brands__campaign_console') }}

),

rename_fields as (  -- rename currency amounts to *_usd and remove currency field

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_name,
        portfolio_name,
        marketplace,
        tenant_id,
        impressions,
        clicks,
        spend as spend_usd,
        _14_day_total_units,
        _14_day_total_orders,
        _14_day_total_sales as _14_day_total_sales_usd

    from sb_campaign_console

),

get_portfolio_code as (

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_name,
        portfolio_name,
        marketplace,
        tenant_id,
        impressions,
        clicks,
        spend_usd,
        _14_day_total_units,
        _14_day_total_orders,
        _14_day_total_sales_usd,

        -- Portfolio Code - should match portfolio_code in dim_products
        case
            when
                tenant_id = 1
                and REGEXP_CONTAINS(portfolio_name, r"-L1$")
                then REGEXP_REPLACE(portfolio_name, r"-L1$", "")
            else portfolio_name
        end as portfolio_code

    from rename_fields

)

select * from get_portfolio_code
