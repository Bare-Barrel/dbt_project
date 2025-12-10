-- int_convert_sd_campaign_amounts_to_usd.sql sd_01

{{ config(materialized='ephemeral') }}

with

sd_campaign as (

    select * from {{ source('sponsored_display', 'campaign') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_campaign_and_fx_rates as (

    select
        sd_c.date,
        sd_c.created_at,
        sd_c.updated_at,
        sd_c.campaign_id,
        sd_c.campaign_name,
        sd_c.campaign_status,
        sd_c.marketplace,
        sd_c.impressions,
        sd_c.clicks,
        sd_c.units_sold_clicks, -- Number of attributed units sold within 14 days of an ad click.
        sd_c.new_to_brand_units_sold_clicks, -- Total number of attributed units ordered as part of new-to-brand sales occurring within 14 days of an ad click.
        sd_c.purchases_clicks, -- Number of attributed conversion events occurring within 14 days of an ad click.
        sd_c.campaign_budget_amount,
        sd_c.campaign_budget_currency_code,
        sd_c.cost,
        sd_c.sales_clicks, -- Total value of sales occurring within 14 days of an ad click.
        sd_c.new_to_brand_sales_clicks, -- Total value of new-to-brand sales occurring within 14 days of an ad click.
        sd_c.tenant_id,

        case
            when sd_c.campaign_budget_currency_code = "USD"
                then 1
            else fx.rate
        end as fx_rate

    from sd_campaign as sd_c

    left join exchange_rates as fx
        on
            sd_c.campaign_budget_currency_code = fx.target
            and DATE(sd_c.date) = fx.recorded_at

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
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        tenant_id,

        SAFE_DIVIDE(campaign_budget_amount, fx_rate) as campaign_budget_amount_usd,
        SAFE_DIVIDE(cost, fx_rate) as cost_usd,
        SAFE_DIVIDE(sales_clicks, fx_rate) as sales_clicks_usd,
        SAFE_DIVIDE(new_to_brand_sales_clicks, fx_rate) as new_to_brand_sales_clicks_usd

    from join_campaign_and_fx_rates

)

select * from convert_amounts_to_usd
