-- int_kpi_tracker__filter_sd_cc.sql kpi_sd_cc_03
-- Advertising reports from console (2023-03-13 - 2023-06-10)

{{ config(materialized='ephemeral') }}

with

sd_campaign_console_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sd_cc_portfolio_code') }}

),

filter_by_date as (

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_name,
        campaign_status,
        portfolio_name,
        portfolio_code,
        marketplace,
        tenant_id,
        cost_type,
        impressions,
        clicks,
        _14_day_total_units_click,
        _14_day_total_orders_click,
        spend_usd,
        _14_day_total_sales_click_usd

    from sd_campaign_console_with_portfolio_code

    where campaign_date <= DATE('2023-06-10')

)

select * from filter_by_date
