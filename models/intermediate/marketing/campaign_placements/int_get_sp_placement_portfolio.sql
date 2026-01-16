-- int_get_sp_placement_portfolio.sql sp_cp_02

{{ config(materialized='ephemeral') }}

with

sp_campaign_placement_usd as (

    select * from {{ ref('int_convert_sp_placement_amounts_to_usd') }}

),

sp_campaigns as (

    select * from {{ source('sponsored_products', 'campaigns') }}

),

ad_portfolios as (

    select * from {{ source('public', 'amazon_advertising_portfolios') }}

),

get_sp_placement_portfolio as (

    select
        sp_cp_usd.date,
        sp_cp_usd.created_at,
        sp_cp_usd.updated_at,
        sp_cp_usd.campaign_id,
        sp_cp_usd.campaign_name,
        sp_cp_usd.campaign_status,
        sp_cp_usd.marketplace,
        sp_cp_usd.placement_classification,
        sp_cp_usd.tenant_id,
        sp_cp_usd.impressions,
        sp_cp_usd.clicks,
        sp_cp_usd.units_sold_clicks_1d,
        sp_cp_usd.units_sold_clicks_7d,
        sp_cp_usd.units_sold_clicks_14d,
        sp_cp_usd.units_sold_clicks_30d,
        sp_cp_usd.purchases_1d,
        sp_cp_usd.purchases_7d,
        sp_cp_usd.purchases_14d,
        sp_cp_usd.purchases_30d,
        sp_cp_usd.campaign_budget_amount_usd,
        sp_cp_usd.cost_usd,
        sp_cp_usd.sales_1d_usd,
        sp_cp_usd.sales_7d_usd,
        sp_cp_usd.sales_14d_usd,
        sp_cp_usd.sales_30d_usd,
        sp_cp_usd.cost_per_click_usd,
        sp_cp_usd.click_through_rate,

        sp_cs.portfolio_id,
        a_p.name as portfolio_name

    from sp_campaign_placement_usd as sp_cp_usd

    left join sp_campaigns as sp_cs
        on sp_cp_usd.campaign_id = sp_cs.campaign_id

    left join ad_portfolios as a_p
        on sp_cs.portfolio_id = a_p.portfolio_id

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
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,
        click_through_rate,

        COALESCE(portfolio_id, 0) as portfolio_id

    from get_sp_placement_portfolio

)

select * from fill_in_nulls
