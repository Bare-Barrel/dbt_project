-- int_kpi_tracker__aggregate_sd_cc.sql kpi_sd_cc_04
-- Advertising reports from console (2023-03-13 - 2023-06-10)

{{ config(materialized='view') }}

with

filtered_sd_campaign_console as (

    select * from {{ ref('int_kpi_tracker__filter_sd_cc') }}

),

aggregate_sd_campaign_console as (

    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,

        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(_14_day_total_units_click) as total_14_day_total_units_click,
        SUM(_14_day_total_orders_click) as total_14_day_total_orders_click,
        SUM(spend_usd) as total_spend_usd,
        SUM(_14_day_total_sales_click_usd) as total_14_day_total_sales_click_usd

    from filtered_sd_campaign_console

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, tenant_id asc, marketplace desc, portfolio_code asc

)

select * from aggregate_sd_campaign_console
