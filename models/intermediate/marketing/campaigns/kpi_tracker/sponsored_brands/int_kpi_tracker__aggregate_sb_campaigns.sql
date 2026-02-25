-- int_kpi_tracker__aggregate_sb_campaigns.sql kpi_sb_c_05
-- API Data source v3 (2023-09-21 - present)

{{ config(materialized='view') }}

with

filtered_sb_campaigns as (

    select * from {{ ref('int_kpi_tracker__filter_sb_campaigns') }}

),

aggregate_sb_campaigns as (

    select
        campaign_date,
        tenant_id,
        marketplace,
        portfolio_code,

        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(units_sold_clicks) as total_units_sold_clicks,
        SUM(new_to_brand_units_sold_clicks) as total_new_to_brand_units_sold_clicks,
        SUM(purchases_clicks) as total_purchases_clicks,
        SUM(top_of_search_impression_share) as total_top_of_search_impression_share,
        SUM(campaign_budget_amount_usd) as total_campaign_budget_amount_usd,
        SUM(cost_usd) as total_cost_usd,
        SUM(sales_clicks_usd) as total_sales_clicks_usd,
        SUM(new_to_brand_sales_clicks_usd) as total_new_to_brand_sales_clicks_usd

    from filtered_sb_campaigns

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, tenant_id asc, marketplace desc, portfolio_code asc

)

select * from aggregate_sb_campaigns
