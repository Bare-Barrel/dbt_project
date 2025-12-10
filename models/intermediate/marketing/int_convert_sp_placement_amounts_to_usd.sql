-- int_convert_sp_placement_amounts_to_usd.sql sp_cp_01

{{ config(materialized='ephemeral') }}

with

sp_campaign_placement as (

    select * from {{ source('sponsored_products', 'campaign_placement') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_campaign_placement_and_fx_rates as (

    select
        sp_cp.date,
        sp_cp.created_at,
        sp_cp.updated_at,
        sp_cp.campaign_id,
        sp_cp.campaign_name,
        sp_cp.campaign_status,
        sp_cp.marketplace,
        sp_cp.placement_classification,
        sp_cp.impressions,
        sp_cp.clicks,
        sp_cp.units_sold_clicks_1d,
        sp_cp.units_sold_clicks_7d,
        sp_cp.units_sold_clicks_14d,
        sp_cp.units_sold_clicks_30d,
        sp_cp.purchases_1d,
        sp_cp.purchases_7d,
        sp_cp.purchases_14d,
        sp_cp.purchases_30d,
        sp_cp.campaign_budget_amount,
        sp_cp.campaign_budget_currency_code,
        sp_cp.cost,
        sp_cp.sales_1d,
        sp_cp.sales_7d,
        sp_cp.sales_14d,
        sp_cp.sales_30d,
        sp_cp.cost_per_click,
        sp_cp.click_through_rate,
        sp_cp.tenant_id,

        case
            when sp_cp.campaign_budget_currency_code = "USD"
                then 1
            else fx.rate
        end as fx_rate

    from sp_campaign_placement as sp_cp

    left join exchange_rates as fx
        on
            sp_cp.campaign_budget_currency_code = fx.target
            and DATE(sp_cp.date) = fx.recorded_at

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
        click_through_rate,

        SAFE_DIVIDE(campaign_budget_amount, fx_rate) as campaign_budget_amount_usd,
        SAFE_DIVIDE(cost, fx_rate) as cost_usd,
        SAFE_DIVIDE(sales_1d, fx_rate) as sales_1d_usd,
        SAFE_DIVIDE(sales_7d, fx_rate) as sales_7d_usd,
        SAFE_DIVIDE(sales_14d, fx_rate) as sales_14d_usd,
        SAFE_DIVIDE(sales_30d, fx_rate) as sales_30d_usd,
        SAFE_DIVIDE(cost_per_click, fx_rate) as cost_per_click_usd

    from join_campaign_placement_and_fx_rates

)

select * from convert_amounts_to_usd
