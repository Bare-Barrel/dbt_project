-- int_get_sb_campaign_v2_portfolio.sql sb_c_v2_02
-- API Data source v2 (2023-06-11 - 2023-09-20)

{{ config(materialized='ephemeral') }}

with

sb_campaign_v2_usd as (

    select * from {{ ref('int_convert_sb_campaign_v2_amounts_to_usd') }}

),

sb_campaigns as (

    select * from {{ ref('stg_sponsored_brands__campaigns') }}

),

ad_portfolios as (

    select * from {{ ref('stg_public__amazon_advertising_portfolios') }}

),

get_sb_v2_portfolio as (

    select
        sb_c_v2_usd.campaign_date,
        sb_c_v2_usd.created_at,
        sb_c_v2_usd.updated_at,
        sb_c_v2_usd.campaign_id,
        sb_c_v2_usd.campaign_name,
        sb_c_v2_usd.campaign_status,
        sb_c_v2_usd.marketplace,
        sb_c_v2_usd.tenant_id,
        sb_c_v2_usd.campaign_budget_type,
        sb_c_v2_usd.impressions,
        sb_c_v2_usd.clicks,
        sb_c_v2_usd.attributed_units_ordered_new_to_brand_14d,
        sb_c_v2_usd.attributed_conversions_14d,
        sb_c_v2_usd.cost_usd,
        sb_c_v2_usd.attributed_sales_14d_usd,

        sb_cs.portfolio_id,
        a_p.portfolio_name

    from sb_campaign_v2_usd as sb_c_v2_usd

    left join sb_campaigns as sb_cs
        on sb_c_v2_usd.campaign_id = sb_cs.campaign_id

    left join ad_portfolios as a_p
        on sb_cs.portfolio_id = a_p.portfolio_id

)

select * from get_sb_v2_portfolio
