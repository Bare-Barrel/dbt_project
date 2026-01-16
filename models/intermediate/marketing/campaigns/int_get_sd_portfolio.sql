-- int_get_sd_portfolio.sql sd_c_02

{{ config(materialized='ephemeral') }}

with

sd_campaign_usd as (

    select * from {{ ref('int_convert_sd_campaign_amounts_to_usd') }}

),

sd_campaigns as (

    select * from {{ source('sponsored_display', 'campaigns') }}

),

ad_portfolios as (

    select * from {{ source('public', 'amazon_advertising_portfolios') }}

),

get_sd_portfolio as (

    select
        sd_c_usd.date,
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
        a_p.name as portfolio_name

    from sd_campaign_usd as sd_c_usd

    left join sd_campaigns as sd_cs
        on sd_c_usd.campaign_id = sd_cs.campaign_id

    left join ad_portfolios as a_p
        on sd_cs.portfolio_id = a_p.portfolio_id

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
        impressions,
        clicks,
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,

        COALESCE(portfolio_id, 0) as portfolio_id

    from get_sd_portfolio

)

select * from fill_in_nulls
