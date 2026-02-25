-- obt_sd_campaigns.sql

{{ config(materialized='view') }}

with

agg_sd_campaigns as (

    select * from {{ ref('int_kpi_tracker__aggregate_sd_campaigns') }}

),

agg_sd_campaign_v2 as (

    select * from {{ ref('int_kpi_tracker__aggregate_sd_campaign_v2') }}

),

agg_sd_cc as (

    select * from {{ ref('int_kpi_tracker__aggregate_sd_cc') }}

),

union_all_sd_campaigns as (

    -- API v3 (2025-01-11 - present)
    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,
        total_impressions,
        total_clicks,
        total_cost_usd,
        total_units_sold_clicks,
        total_purchases_clicks,
        total_sales_clicks_usd
    from agg_sd_campaigns

    union all

    -- API v2 (2023-06-11 - 2025-01-10)
    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,
        total_impressions,
        total_clicks,
        total_cost_usd,
        total_attributed_units_ordered_14d as total_units_sold_clicks,
        total_attributed_conversions_14d as total_purchases_clicks,
        total_attributed_sales_14d_usd as total_sales_clicks_usd
    from agg_sd_campaign_v2

    union all

    -- Advertising reports from console (2023-03-13 - 2023-06-10)
    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,
        total_impressions,
        total_clicks,
        total_spend_usd as total_cost_usd,
        total_14_day_total_units_click as total_units_sold_clicks,
        total_14_day_total_orders_click as total_purchases_clicks,
        total_14_day_total_sales_click_usd as total_sales_clicks_usd
    from agg_sd_cc

)

select * from union_all_sd_campaigns
