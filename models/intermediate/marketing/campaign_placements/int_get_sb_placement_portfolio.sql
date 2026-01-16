-- int_get_sb_placement_portfolio.sql sb_cp_02

{{ config(materialized='ephemeral') }}

with

sb_campaign_placement_usd as (

    select * from {{ ref('int_convert_sb_placement_amounts_to_usd') }}

),

sb_campaigns as (

    select * from {{ source('sponsored_brands', 'campaigns') }}

),

ad_portfolios as (

    select * from {{ source('public', 'amazon_advertising_portfolios') }}

),

get_sb_placement_portfolio as (

    select
        sb_cp_usd.date,
        sb_cp_usd.created_at,
        sb_cp_usd.updated_at,
        sb_cp_usd.campaign_id,
        sb_cp_usd.campaign_name,
        sb_cp_usd.campaign_status,
        sb_cp_usd.marketplace,
        sb_cp_usd.placement_classification,
        sb_cp_usd.tenant_id,
        sb_cp_usd.impressions,
        sb_cp_usd.clicks,
        sb_cp_usd.units_sold_clicks,
        sb_cp_usd.new_to_brand_units_sold_clicks,
        sb_cp_usd.purchases_clicks,
        sb_cp_usd.campaign_budget_amount_usd,
        sb_cp_usd.cost_usd,
        sb_cp_usd.sales_clicks_usd,
        sb_cp_usd.new_to_brand_sales_clicks_usd,

        sb_cs.portfolio_id,
        a_p.name as portfolio_name

    from sb_campaign_placement_usd as sb_cp_usd

    left join sb_campaigns as sb_cs
        on sb_cp_usd.campaign_id = sb_cs.campaign_id

    left join ad_portfolios as a_p
        on sb_cs.portfolio_id = a_p.portfolio_id

),

fill_in_nulls as (

    select
        date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_name,
        marketplace,
        placement_classification,
        tenant_id,
        impressions,
        clicks,
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,

        COALESCE(portfolio_id, 0) as portfolio_id

    from get_sb_placement_portfolio

)

select * from fill_in_nulls
