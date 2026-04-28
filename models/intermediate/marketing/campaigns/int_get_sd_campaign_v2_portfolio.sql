-- int_get_sd_campaign_v2_portfolio.sql sd_c_v2_02
-- API v2 (2023-06-11 - 2025-01-10)

{{ config(materialized='ephemeral') }}

with

sd_campaign_v2_usd as (

    select * from {{ ref('int_convert_sd_campaign_v2_amounts_to_usd') }}

),

sd_campaigns as (

    select * from {{ ref('stg_sponsored_display__campaigns') }}

),

ad_portfolios_v3 as (

    select * from {{ ref('stg_amazon_ads_api__amazon_advertising_portfolios_v3') }}

),

get_sd_v2_portfolio as (

    select
        sd_c_v2_usd.campaign_date,
        sd_c_v2_usd.created_at,
        sd_c_v2_usd.updated_at,
        sd_c_v2_usd.campaign_id,
        sd_c_v2_usd.campaign_name,
        sd_c_v2_usd.campaign_status,
        sd_c_v2_usd.marketplace,
        sd_c_v2_usd.tenant_id,
        sd_c_v2_usd.cost_type,
        sd_c_v2_usd.impressions,
        sd_c_v2_usd.clicks,
        sd_c_v2_usd.cost_usd,
        sd_c_v2_usd.attributed_units_ordered_14d,
        sd_c_v2_usd.attributed_conversions_14d,
        sd_c_v2_usd.attributed_sales_14d_usd,

        sd_cs.portfolio_id,
        a_p_v3.portfolio_name

    from sd_campaign_v2_usd as sd_c_v2_usd

    left join sd_campaigns as sd_cs
        on sd_c_v2_usd.campaign_id = sd_cs.campaign_id

    left join ad_portfolios_v3 as a_p_v3
        on sd_cs.portfolio_id = a_p_v3.portfolio_id
)

select * from get_sd_v2_portfolio
