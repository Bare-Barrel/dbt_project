-- int_kpi_tracker__union_all_sb_campaigns.sql

{{ config(materialized='view') }}

with

agg_sb_campaigns as (

    select * from {{ ref('int_kpi_tracker__aggregate_sb_campaigns') }}

),

agg_sb_campaign_v2 as (

    select * from {{ ref('int_kpi_tracker__aggregate_sb_campaign_v2') }}

),

agg_sb_cc as (

    select * from {{ ref('int_kpi_tracker__aggregate_sb_cc') }}

),

union_all_sb_campaigns as (

    -- API Data source v3 (2023-09-21 - present)
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
    from agg_sb_campaigns

    union all

    -- API Data source v2 (2023-06-11 - 2023-09-20)
    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,
        total_impressions,
        total_clicks,
        total_cost_usd,
        total_attributed_units_ordered_new_to_brand_14d as total_units_sold_clicks,
        total_attributed_conversions_14d as total_purchases_clicks,
        total_attributed_sales_14d_usd as total_sales_clicks_usd
    from agg_sb_campaign_v2

    union all

    -- Advertising reports from advertising console (2022-07-10 - 2023-06-10)
    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,
        total_impressions,
        total_clicks,
        total_spend_usd as total_cost_usd,
        total_14_day_total_units as total_units_sold_clicks,
        total_14_day_total_orders as total_purchases_clicks,
        total_14_day_total_sales_usd as total_sales_clicks_usd
    from agg_sb_cc

)

select * from union_all_sb_campaigns
