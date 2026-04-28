-- int_get_sd_portfolio.sql sd_c_02
-- API v3 (2025-01-11 - present)

{{ config(materialized='ephemeral') }}

with

sd_campaign_usd as (

    select * from {{ ref('int_convert_sd_campaign_amounts_to_usd') }}

),

sd_campaigns as (

    select * from {{ ref('stg_sponsored_display__campaigns') }}

),

ad_portfolios_v3 as (

    select * from {{ ref('stg_amazon_ads_api__amazon_advertising_portfolios_v3') }}

),

get_sd_portfolio as (

    select
        sd_c_usd.campaign_date,
        sd_c_usd.created_at,
        sd_c_usd.updated_at,
        sd_c_usd.campaign_id,
        sd_c_usd.campaign_name,
        sd_c_usd.campaign_status,
        sd_c_usd.marketplace,
        sd_c_usd.impressions,
        sd_c_usd.clicks,
        sd_c_usd.units_sold_clicks,
        sd_c_usd.new_to_brand_units_sold_clicks,
        sd_c_usd.purchases_clicks,
        sd_c_usd.tenant_id,
        sd_c_usd.campaign_budget_amount_usd,
        sd_c_usd.cost_usd,
        sd_c_usd.sales_clicks_usd,
        sd_c_usd.new_to_brand_sales_clicks_usd,

        sd_cs.portfolio_id,
        a_p_v3.portfolio_name

    from sd_campaign_usd as sd_c_usd

    left join sd_campaigns as sd_cs
        on sd_c_usd.campaign_id = sd_cs.campaign_id

    left join ad_portfolios_v3 as a_p_v3
        on sd_cs.portfolio_id = a_p_v3.portfolio_id

)

select * from get_sd_portfolio
