-- int_get_sb_portfolio.sql sb_c_02

{{ config(materialized='ephemeral') }}

with

sb_campaign_usd as (

    select * from {{ ref('int_convert_sb_campaign_amounts_to_usd') }}

),

sb_campaigns as (

    select * from {{ source('sponsored_brands', 'campaigns') }}

),

ad_portfolios as (

    select * from {{ source('public', 'amazon_advertising_portfolios') }}

),

get_sb_placement_and_portfolio as (

    select
        sb_c_usd.date,
        sb_c_usd.created_at,
        sb_c_usd.updated_at,
        sb_c_usd.campaign_id,
        sb_c_usd.campaign_name,
        sb_c_usd.campaign_status,
        sb_cs.portfolio_id,
        a_p.name as portfolio_name,
        sb_c_usd.marketplace,
        sb_c_usd.impressions,
        sb_c_usd.clicks,
        sb_c_usd.units_sold_clicks,
        sb_c_usd.new_to_brand_units_sold_clicks,
        sb_c_usd.purchases_clicks,
        sb_c_usd.top_of_search_impression_share,
        sb_c_usd.tenant_id,
        sb_c_usd.campaign_budget_amount_usd,
        sb_c_usd.cost_usd,
        sb_c_usd.sales_clicks_usd,
        sb_c_usd.new_to_brand_sales_clicks_usd

    from sb_campaign_usd as sb_c_usd

    left join sb_campaigns as sb_cs
        on sb_c_usd.campaign_id = sb_cs.campaign_id

    left join ad_portfolios as a_p
        on sb_cs.portfolio_id = a_p.portfolio_id

)

select * from get_sb_placement_and_portfolio
