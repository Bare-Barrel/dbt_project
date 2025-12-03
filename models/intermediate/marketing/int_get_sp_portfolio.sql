-- int_get_sp_portfolio.sql sp_02

{{ config(materialized='ephemeral') }}

with

sp_campaign_usd as (

    select * from {{ ref('int_convert_sp_campaign_amounts_to_usd') }}

),

{# sp_campaign_placement as (   TODO: figure out how to get placement (campaign_id:placement is one:many)

    select * from {{ source('sponsored_products', 'campaign_placement') }}

), #}

sp_campaigns as (

    select * from {{ source('sponsored_products', 'campaigns') }}

),

ad_portfolios as (

    select * from {{ source('public', 'amazon_advertising_portfolios') }}

),

{# unique_sp_campaign_placement as (

    select
        campaign_id,
        placement_classification,
        COUNT(*) as count

    from sp_campaign_placement

    where cost != 0

    group by campaign_id, placement_classification

), #}

get_sp_placement_and_portfolio as (

    select
        sp_c_usd.date,
        sp_c_usd.created_at,
        sp_c_usd.updated_at,
        sp_c_usd.campaign_id,
        sp_c_usd.campaign_name,
        sp_c_usd.campaign_status,
        sp_cs.portfolio_id,
        a_p.name as portfolio_name,
        sp_c_usd.marketplace,
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
        sp_c_usd.tenant_id,
        sp_c_usd.campaign_budget_amount_usd,
        sp_c_usd.cost_usd,
        sp_c_usd.sales_1d_usd,
        sp_c_usd.sales_7d_usd,
        sp_c_usd.sales_14d_usd,
        sp_c_usd.sales_30d_usd,
        sp_c_usd.cost_per_click_usd
    {# u_sp_c_p.placement_classification #}

    from sp_campaign_usd as sp_c_usd

    left join sp_campaigns as sp_cs
        on sp_c_usd.campaign_id = sp_cs.campaign_id

    left join ad_portfolios as a_p
        on sp_cs.portfolio_id = a_p.portfolio_id

    {# left join unique_sp_campaign_placement as u_sp_c_p
        on sp_c_usd.campaign_id = u_sp_c_p.campaign_id #}

)

select * from get_sp_placement_and_portfolio
