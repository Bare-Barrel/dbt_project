-- int_convert_sb_placement_amounts_to_usd.sql sb_cp_01

{{ config(materialized='ephemeral') }}

with

sb_campaign_placement as (

    select * from {{ source('sponsored_brands', 'campaign_placement') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_campaign_and_fx_rates as (

    select
        sb_cp.date,
        sb_cp.created_at,
        sb_cp.updated_at,
        sb_cp.campaign_id,
        sb_cp.campaign_name,
        sb_cp.campaign_status,
        sb_cp.marketplace,
        sb_cp.placement_classification,
        sb_cp.impressions,
        sb_cp.clicks,
        sb_cp.units_sold_clicks, -- Number of attributed units sold within 14 days of an ad click.
        sb_cp.new_to_brand_units_sold_clicks, -- Total number of attributed units ordered as part of new-to-brand sales occurring within 14 days of an ad click.
        sb_cp.purchases_clicks, -- Number of attributed conversion events occurring within 14 days of an ad click.
        sb_cp.campaign_budget_amount,
        sb_cp.campaign_budget_currency_code,
        sb_cp.cost,
        sb_cp.sales_clicks, -- Total value of sales occurring within 14 days of an ad click.
        sb_cp.new_to_brand_sales_clicks, -- Total value of new-to-brand sales occurring within 14 days of an ad click.
        sb_cp.tenant_id,

        case
            when sb_cp.campaign_budget_currency_code = "USD"
                then 1
            else fx.rate
        end as fx_rate

    from sb_campaign_placement as sb_cp

    left join exchange_rates as fx
        on
            sb_cp.campaign_budget_currency_code = fx.target
            and DATE(sb_cp.date) = fx.recorded_at

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
