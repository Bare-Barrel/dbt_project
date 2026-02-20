-- int_get_sp_portfolio.sql sp_c_02

{{ config(materialized='ephemeral') }}

with

sp_campaign_usd as (

    select * from {{ ref('int_convert_sp_campaign_amounts_to_usd') }}

),

sp_campaigns as (

    select * from {{ ref('stg_sponsored_products__campaigns') }}

),

ad_portfolios as (

    select * from {{ ref('stg_public__amazon_advertising_portfolios') }}

),

get_sp_portfolio as (

    select
        sp_c_usd.campaign_date,
        sp_c_usd.created_at,
        sp_c_usd.updated_at,
        sp_c_usd.campaign_id,
        sp_c_usd.campaign_name,
        sp_c_usd.campaign_status,
        sp_c_usd.marketplace,
        sp_c_usd.tenant_id,
        sp_c_usd.impressions,
        sp_c_usd.clicks,
        sp_c_usd.units_sold_clicks_1d,
        sp_c_usd.units_sold_clicks_7d,
        sp_c_usd.units_sold_clicks_14d,
        sp_c_usd.units_sold_clicks_30d,
        sp_c_usd.purchases_1d,
        sp_c_usd.purchases_7d,
        sp_c_usd.purchases_14d,
        sp_c_usd.purchases_30d,
        sp_c_usd.click_through_rate,
        sp_c_usd.top_of_search_impression_share,
        sp_c_usd.campaign_budget_amount_usd,
        sp_c_usd.cost_usd,
        sp_c_usd.sales_1d_usd,
        sp_c_usd.sales_7d_usd,
        sp_c_usd.sales_14d_usd,
        sp_c_usd.sales_30d_usd,
        sp_c_usd.cost_per_click_usd,

        sp_cs.portfolio_id,
        a_p.portfolio_name

    from sp_campaign_usd as sp_c_usd

    left join sp_campaigns as sp_cs
        on sp_c_usd.campaign_id = sp_cs.campaign_id

    left join ad_portfolios as a_p
        on sp_cs.portfolio_id = a_p.portfolio_id

)

select * from get_sp_portfolio
