-- int_convert_sd_campaign_amounts_to_usd.sql sd_c_01
-- API v3 (2025-01-11 - present)

{{ config(materialized='ephemeral') }}

with

sd_campaign as (

    select * from {{ ref('stg_sponsored_display__campaign') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

convert_sd_amounts_to_usd as (

    select
        sd_c.campaign_date,
        sd_c.created_at,
        sd_c.updated_at,
        sd_c.campaign_id,
        sd_c.campaign_name,
        sd_c.campaign_status,
        sd_c.marketplace,
        sd_c.tenant_id,
        sd_c.impressions,
        sd_c.clicks,
        sd_c.units_sold_clicks, -- Number of attributed units sold within 14 days of an ad click.
        sd_c.new_to_brand_units_sold_clicks, -- Total number of attributed units ordered as part of new-to-brand sales occurring within 14 days of an ad click.
        sd_c.purchases_clicks, -- Number of attributed conversion events occurring within 14 days of an ad click.

        -- Amounts
        {# sd_c.campaign_budget_currency_code,
        sd_c.campaign_budget_amount,
        sd_c.cost,
        sd_c.sales_clicks, -- Total value of sales occurring within 14 days of an ad click.
        sd_c.new_to_brand_sales_clicks, -- Total value of new-to-brand sales occurring within 14 days of an ad click. #}

        -- campaign_budget_amount_usd
        case
            when sd_c.campaign_budget_currency_code = "USD"
                then sd_c.campaign_budget_amount
            else SAFE_DIVIDE(sd_c.campaign_budget_amount, fx.rate)
        end as campaign_budget_amount_usd,

        -- cost_usd
        case
            when sd_c.campaign_budget_currency_code = "USD"
                then sd_c.cost
            else SAFE_DIVIDE(sd_c.cost, fx.rate)
        end as cost_usd,

        -- sales_clicks_usd
        case
            when sd_c.campaign_budget_currency_code = "USD"
                then sd_c.sales_clicks
            else SAFE_DIVIDE(sd_c.sales_clicks, fx.rate)
        end as sales_clicks_usd,

        -- new_to_brand_sales_clicks_usd
        case
            when sd_c.campaign_budget_currency_code = "USD"
                then sd_c.new_to_brand_sales_clicks
            else SAFE_DIVIDE(sd_c.new_to_brand_sales_clicks, fx.rate)
        end as new_to_brand_sales_clicks_usd

    from sd_campaign as sd_c

    left join exchange_rates as fx
        on
            sd_c.campaign_budget_currency_code = fx.target
            and DATE(sd_c.campaign_date) = fx.recorded_at

)

select * from convert_sd_amounts_to_usd
