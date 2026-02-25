-- int_kpi_tracker__get_sd_cc_portfolio_code.sql kpi_sd_cc_02
-- Advertising reports from console (2023-03-13 - 2023-06-10)

{{ config(materialized='ephemeral') }}

with

sd_campaign_console_usd as (

    select * from {{ ref('int_convert_sd_cc_amounts_to_usd') }}

),

get_portfolio_code as (

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
        spend_usd,
        _14_day_total_sales_click_usd,

        -- Portfolio Code
        portfolio_name as portfolio_code

    from sd_campaign_console_usd

)

select * from get_portfolio_code
