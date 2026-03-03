-- int_convert_sp_advertised_product_amounts_to_usd.sql

{{ config(materialized='view') }}

with

sp_advertised_product as (

    select * from {{ ref('stg_sponsored_products__advertised_product') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

convert_amounts_to_usd as ( -- retain all 14d amounts

    select
        sp_ap.campaign_date,
        sp_ap.created_at,
        sp_ap.updated_at,
        sp_ap.ad_id,
        sp_ap.ad_group_id,
        sp_ap.campaign_id,
        sp_ap.portfolio_id,
        sp_ap.tenant_id,
        sp_ap.ad_group_name,
        sp_ap.campaign_name,
        sp_ap.campaign_status,
        sp_ap.marketplace,
        sp_ap.advertised_sku,
        sp_ap.advertised_asin,
        sp_ap.campaign_budget_type,
        sp_ap.impressions,
        sp_ap.clicks,
        sp_ap.purchases_14d,
        sp_ap.purchases_same_sku_14d,
        sp_ap.acos_clicks_14d,
        sp_ap.roas_clicks_14d,
        sp_ap.click_through_rate,
        sp_ap.units_sold_clicks_14d,
        sp_ap.units_sold_same_sku_14d,
        sp_ap.kindle_edition_normalized_pages_read_14d,
        sp_ap.kindle_edition_normalized_pages_royalties_14d,

        -- Amounts
        {# sp_ap.campaign_budget_currency_code,
        sp_ap.cost,
        sp_ap.spend,
        sp_ap.sales_14d,
        sp_ap.cost_per_click,
        sp_ap.campaign_budget_amount,
        sp_ap.attributed_sales_same_sku_14d, #}

        -- cost_usd
        case
            when sp_ap.campaign_budget_currency_code = "USD"
                then sp_ap.cost
            else SAFE_DIVIDE(sp_ap.cost, fx.rate)
        end as cost_usd,

        -- spend_usd
        case
            when sp_ap.campaign_budget_currency_code = "USD"
                then sp_ap.spend
            else SAFE_DIVIDE(sp_ap.spend, fx.rate)
        end as spend_usd,

        -- sales_14d_usd
        case
            when sp_ap.campaign_budget_currency_code = "USD"
                then sp_ap.sales_14d
            else SAFE_DIVIDE(sp_ap.sales_14d, fx.rate)
        end as sales_14d_usd,

        -- cost_per_click_usd
        case
            when sp_ap.campaign_budget_currency_code = "USD"
                then sp_ap.cost_per_click
            else SAFE_DIVIDE(sp_ap.cost_per_click, fx.rate)
        end as cost_per_click_usd,

        -- campaign_budget_amount_usd
        case
            when sp_ap.campaign_budget_currency_code = "USD"
                then sp_ap.campaign_budget_amount
            else SAFE_DIVIDE(sp_ap.campaign_budget_amount, fx.rate)
        end as campaign_budget_amount_usd,

        -- attributed_sales_same_sku_14d_usd
        case
            when sp_ap.campaign_budget_currency_code = "USD"
                then sp_ap.attributed_sales_same_sku_14d
            else SAFE_DIVIDE(sp_ap.attributed_sales_same_sku_14d, fx.rate)
        end as attributed_sales_same_sku_14d_usd

    from sp_advertised_product as sp_ap

    left join exchange_rates as fx
        on
            sp_ap.campaign_budget_currency_code = fx.target
            and DATE(sp_ap.campaign_date) = fx.recorded_at

)

select * from convert_amounts_to_usd
