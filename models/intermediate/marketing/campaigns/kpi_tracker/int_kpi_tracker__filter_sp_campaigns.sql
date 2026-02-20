-- int_kpi_tracker__filter_sp_campaigns.sql kpi_sp_c_04
-- API data source. (04-02-2023 to present)

{{ config(materialized='ephemeral') }}

with

sp_campaigns_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sp_portfolio_code') }}

),

filter_out_zero_impressions as (

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        portfolio_code,
        marketplace,
        tenant_id,
        impressions,
        clicks,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        click_through_rate,
        top_of_search_impression_share,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd

    from sp_campaigns_with_portfolio_code

    where impressions > 0

)

select * from filter_out_zero_impressions
