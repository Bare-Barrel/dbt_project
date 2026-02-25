-- int_convert_sd_cc_amounts_to_usd.sql sd_cc_01
-- Advertising reports from console (2023-03-13 - 2023-06-10)

{{ config(materialized='ephemeral') }}

with

sd_campaign_console as (

    select * from {{ ref('stg_sponsored_display__campaign_console') }}

),

rename_sd_cc_amounts_to_usd as (

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_name,
        campaign_status,
        portfolio_name,
        marketplace,
        tenant_id,
        cost_type,
        impressions,
        clicks,
        _14_day_total_units_click,
        _14_day_total_orders_click,

        -- Amounts -- all amounts are already in USD
        spend as spend_usd,
        _14_day_total_sales_click as _14_day_total_sales_click_usd

    from sd_campaign_console

)

select * from rename_sd_cc_amounts_to_usd
