-- int_convert_sp_cc_amounts_to_usd.sql sp_cc_01
-- Advertising reports data source for (06-27-2022 to 2024-04-01)

{{ config(materialized='ephemeral') }}

with

sp_campaign_console as (

    select * from {{ ref('stg_sponsored_products__campaign_console') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_campaign_and_fx_rates as (

    select
        sp_cc.campaign_date,
        sp_cc.created_at,
        sp_cc.updated_at,
        sp_cc.tenant_id,
        sp_cc.campaign_name,
        sp_cc.campaign_type,
        sp_cc.campaign_status,
        sp_cc.portfolio_name,
        sp_cc.marketplace,
        sp_cc.country,
        sp_cc.targeting_type,
        sp_cc.bidding_strategy,
        sp_cc.impressions,
        sp_cc.clicks,
        sp_cc._7_day_total_orders,
        sp_cc.click_thru_rate,
        sp_cc.last_year_impressions,
        sp_cc.last_year_clicks,
        sp_cc.total_advertising_cost_of_sales,
        sp_cc.total_return_on_advertising_spend,

        -- Amounts
        {# sp_cc.spend,
        sp_cc.budget,
        sp_cc._7_day_total_sales,
        sp_cc.cost_per_click,
        sp_cc.last_year_spend,
        sp_cc.last_year_cost_per_click, #}

        -- spend_usd
        case
            when sp_cc.currency = "USD"
                then sp_cc.spend
            else SAFE_DIVIDE(sp_cc.spend, fx.rate)
        end as spend_usd,

        -- budget_usd
        case
            when sp_cc.currency = "USD"
                then sp_cc.budget
            else SAFE_DIVIDE(sp_cc.budget, fx.rate)
        end as budget_usd,

        -- _7_day_total_sales_usd
        case
            when sp_cc.currency = "USD"
                then sp_cc._7_day_total_sales
            else SAFE_DIVIDE(sp_cc._7_day_total_sales, fx.rate)
        end as _7_day_total_sales_usd,

        -- cost_per_click_usd
        case
            when sp_cc.currency = "USD"
                then sp_cc.cost_per_click
            else SAFE_DIVIDE(sp_cc.cost_per_click, fx.rate)
        end as cost_per_click_usd,

        -- last_year_spend_usd
        case
            when sp_cc.currency = "USD"
                then sp_cc.last_year_spend
            else SAFE_DIVIDE(sp_cc.last_year_spend, fx.rate)
        end as last_year_spend_usd,

        -- last_year_cost_per_click_usd
        case
            when sp_cc.currency = "USD"
                then sp_cc.last_year_cost_per_click
            else SAFE_DIVIDE(sp_cc.last_year_cost_per_click, fx.rate)
        end as last_year_cost_per_click_usd

    from sp_campaign_console as sp_cc

    left join exchange_rates as fx
        on
            sp_cc.currency = fx.target
            and sp_cc.campaign_date = fx.recorded_at

)

select * from join_campaign_and_fx_rates
