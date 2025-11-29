-- int_convert_sp_campaign_amounts_to_usd.sql sp_01

{{ config(materialized='ephemeral') }}

with

sp_campaign as (

    select * from {{ source('sponsored_products', 'campaign') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_campaign_and_fx_rates as (

    select
        sp_c.date,
        sp_c.created_at,
        sp_c.updated_at,
        sp_c.campaign_id,
        sp_c.campaign_name,
        sp_c.campaign_status,
        sp_c.marketplace,
        sp_c.impressions,
        sp_c.clicks,
        sp_c.units_sold_clicks_1d,
        sp_c.units_sold_clicks_7d,
        sp_c.units_sold_clicks_14d,
        sp_c.units_sold_clicks_30d,
        sp_c.purchases_1d,
        sp_c.purchases_7d,
        sp_c.purchases_14d,
        sp_c.purchases_30d,
        sp_c.campaign_budget_amount,
        sp_c.campaign_budget_currency_code,
        sp_c.cost,
        sp_c.sales_1d,
        sp_c.sales_7d,
        sp_c.sales_14d,
        sp_c.sales_30d,
        sp_c.cost_per_click,
        sp_c.click_through_rate,
        sp_c.top_of_search_impression_share,
        sp_c.tenant_id,

        case
            when sp_c.campaign_budget_currency_code = "USD"
                then 1
            else fx.rate
        end as fx_rate

    from sp_campaign as sp_c

    left join exchange_rates as fx
        on
            sp_c.campaign_budget_currency_code = fx.target
            and DATE(sp_c.updated_at) = fx.recorded_at

),

convert_amounts_to_usd as (

    select
        date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        marketplace,
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
        tenant_id,

        SAFE_DIVIDE(campaign_budget_amount, fx_rate) as campaign_budget_amount_usd,
        SAFE_DIVIDE(cost, fx_rate) as cost_usd,
        SAFE_DIVIDE(sales_1d, fx_rate) as sales_1d_usd,
        SAFE_DIVIDE(sales_7d, fx_rate) as sales_7d_usd,
        SAFE_DIVIDE(sales_14d, fx_rate) as sales_14d_usd,
        SAFE_DIVIDE(sales_30d, fx_rate) as sales_30d_usd,
        SAFE_DIVIDE(cost_per_click, fx_rate) as cost_per_click_usd

    from join_campaign_and_fx_rates

)

select * from convert_amounts_to_usd
