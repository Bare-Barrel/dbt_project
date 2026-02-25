-- int_kpi_tracker__aggregate_sd_campaigns.sql kpi_sd_c_04
-- API v3 (2025-01-11 - present)

{{ config(materialized='view') }}

with

sd_campaigns_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sd_portfolio_code') }}

),

aggregate_sd_campaigns as (

    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,

        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(units_sold_clicks) as total_units_sold_clicks,
        SUM(new_to_brand_units_sold_clicks) as total_new_to_brand_units_sold_clicks,
        SUM(purchases_clicks) as total_purchases_clicks,
        SUM(campaign_budget_amount_usd) as total_campaign_budget_amount_usd,
        SUM(cost_usd) as total_cost_usd,
        SUM(sales_clicks_usd) as total_sales_clicks_usd,
        SUM(new_to_brand_sales_clicks_usd) as total_new_to_brand_sales_clicks_usd

    from sd_campaigns_with_portfolio_code

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, tenant_id asc, marketplace desc, portfolio_code asc

)

select * from aggregate_sd_campaigns
