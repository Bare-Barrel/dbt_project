-- int_kpi_tracker__aggregate_sp_campaigns.sql kpi_sp_c_05
-- API data source. (04-02-2023 to present)

{{ config(materialized='view') }}

with

sp_campaigns_with_non_zero_impressions as (

    select * from {{ ref('int_kpi_tracker__filter_sp_campaigns') }}

),

aggregate_sp_campaigns as (

    select
        campaign_date,
        tenant_id,
        marketplace,
        portfolio_code,

        -- Amounts
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(units_sold_clicks_1d) as total_units_sold_clicks_1d,
        SUM(units_sold_clicks_7d) as total_units_sold_clicks_7d,
        SUM(units_sold_clicks_14d) as total_units_sold_clicks_14d,
        SUM(units_sold_clicks_30d) as total_units_sold_clicks_30d,
        SUM(purchases_1d) as total_purchases_1d,
        SUM(purchases_7d) as total_purchases_7d,
        SUM(purchases_14d) as total_purchases_14d,
        SUM(purchases_30d) as total_purchases_30d,
        SUM(click_through_rate) as total_click_through_rate,  -- check before using!!
        SUM(top_of_search_impression_share) as total_top_of_search_impression_share,  -- check before using!!
        SUM(campaign_budget_amount_usd) as total_campaign_budget_amount_usd,
        SUM(cost_usd) as total_cost_usd,
        SUM(sales_1d_usd) as total_sales_1d_usd,
        SUM(sales_7d_usd) as total_sales_7d_usd,
        SUM(sales_14d_usd) as total_sales_14d_usd,
        SUM(sales_30d_usd) as total_sales_30d_usd,
        SUM(cost_per_click_usd) as total_cost_per_click_usd

    from sp_campaigns_with_non_zero_impressions

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, marketplace desc, portfolio_code asc

)

select * from aggregate_sp_campaigns
