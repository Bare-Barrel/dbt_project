-- int_convert_sp_campaign_amounts_to_usd.sql sp_c_01
-- API data source. (04-02-2023 to present)

{{ config(materialized='ephemeral') }}

with

sp_campaign as (

    select * from {{ ref('stg_sponsored_products__campaign') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_campaign_and_fx_rates as (

    select
        sp_c.campaign_date,
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

        -- Amounts
        {# campaign_budget_amount,
        cost,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        cost_per_click #}

        -- campaign_budget_amount_usd
        case
            when sp_c.campaign_budget_currency_code = "USD"
                then sp_c.campaign_budget_amount
            else SAFE_DIVIDE(sp_c.campaign_budget_amount, fx.rate)
        end as campaign_budget_amount_usd,

        -- cost_usd
        case
            when sp_c.campaign_budget_currency_code = "USD"
                then sp_c.cost
            else SAFE_DIVIDE(sp_c.cost, fx.rate)
        end as cost_usd,

        -- sales_1d_usd
        case
            when sp_c.campaign_budget_currency_code = "USD"
                then sp_c.sales_1d
            else SAFE_DIVIDE(sp_c.sales_1d, fx.rate)
        end as sales_1d_usd,

        -- sales_7d_usd
        case
            when sp_c.campaign_budget_currency_code = "USD"
                then sp_c.sales_7d
            else SAFE_DIVIDE(sp_c.sales_7d, fx.rate)
        end as sales_7d_usd,

        -- sales_14d_usd
        case
            when sp_c.campaign_budget_currency_code = "USD"
                then sp_c.sales_14d
            else SAFE_DIVIDE(sp_c.sales_14d, fx.rate)
        end as sales_14d_usd,

        -- sales_30d_usd
        case
            when sp_c.campaign_budget_currency_code = "USD"
                then sp_c.sales_30d
            else SAFE_DIVIDE(sp_c.sales_30d, fx.rate)
        end as sales_30d_usd,

        -- cost_per_click_usd
        case
            when sp_c.campaign_budget_currency_code = "USD"
                then sp_c.cost_per_click
            else SAFE_DIVIDE(sp_c.cost_per_click, fx.rate)
        end as cost_per_click_usd

    from sp_campaign as sp_c

    left join exchange_rates as fx
        on
            sp_c.campaign_budget_currency_code = fx.target
            and sp_c.campaign_date = fx.recorded_at

)

select * from join_campaign_and_fx_rates
