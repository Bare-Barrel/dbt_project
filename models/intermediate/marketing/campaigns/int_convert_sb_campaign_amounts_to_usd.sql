-- int_convert_sb_campaign_amounts_to_usd.sql sb_c_01

{{ config(materialized='ephemeral') }}

with

sb_campaign as (

    select * from {{ ref('stg_sponsored_brands__campaign') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

convert_sb_campaign_amounts_to_usd as (

    select
        sb_c.campaign_date,
        sb_c.created_at,
        sb_c.updated_at,
        sb_c.campaign_id,
        sb_c.campaign_name,
        sb_c.campaign_status,
        sb_c.marketplace,
        sb_c.tenant_id,
        sb_c.impressions,
        sb_c.clicks,
        sb_c.units_sold_clicks, -- Number of attributed units sold within 14 days of an ad click.
        sb_c.new_to_brand_units_sold_clicks, -- Total number of attributed units ordered as part of new-to-brand sales occurring within 14 days of an ad click.
        sb_c.purchases_clicks, -- Number of attributed conversion events occurring within 14 days of an ad click.
        sb_c.top_of_search_impression_share,

        -- Amounts
        {# sb_c.campaign_budget_currency_code,
        sb_c.campaign_budget_amount,
        sb_c.cost,
        sb_c.sales_clicks,
        sb_c.new_to_brand_sales_clicks, #}

        -- campaign_budget_amount_usd
        case
            when sb_c.campaign_budget_currency_code = "USD"
                then sb_c.campaign_budget_amount
            else SAFE_DIVIDE(sb_c.campaign_budget_amount, fx.rate)
        end as campaign_budget_amount_usd,

        -- cost_usd
        case
            when sb_c.campaign_budget_currency_code = "USD"
                then sb_c.cost
            else SAFE_DIVIDE(sb_c.cost, fx.rate)
        end as cost_usd,

        -- sales_clicks_usd -- Total value of sales occurring within 14 days of an ad click.
        case
            when sb_c.campaign_budget_currency_code = "USD"
                then sb_c.sales_clicks
            else SAFE_DIVIDE(sb_c.sales_clicks, fx.rate)
        end as sales_clicks_usd,

        -- new_to_brand_sales_clicks_usd -- Total value of new-to-brand sales occurring within 14 days of an ad click.
        case
            when sb_c.campaign_budget_currency_code = "USD"
                then sb_c.new_to_brand_sales_clicks
            else SAFE_DIVIDE(sb_c.new_to_brand_sales_clicks, fx.rate)
        end as new_to_brand_sales_clicks_usd

    from sb_campaign as sb_c

    left join exchange_rates as fx
        on
            sb_c.campaign_budget_currency_code = fx.target
            and DATE(sb_c.campaign_date) = fx.recorded_at

)

select * from convert_sb_campaign_amounts_to_usd
