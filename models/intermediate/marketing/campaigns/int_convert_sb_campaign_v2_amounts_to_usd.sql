-- int_convert_sb_campaign_v2_amounts_to_usd.sql sb_c_v2_01
-- API Data source v2 (2023-06-11 - 2023-09-20)

{{ config(materialized='ephemeral') }}

with

sb_campaign_v2 as (

    select * from {{ ref('stg_sponsored_brands__campaign_v2') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

convert_sb_campaign_v2_amounts_to_usd as (

    select
        sp_c_v2.campaign_date,
        sp_c_v2.created_at,
        sp_c_v2.updated_at,
        sp_c_v2.campaign_id,
        sp_c_v2.campaign_name,
        sp_c_v2.campaign_status,
        sp_c_v2.marketplace,
        sp_c_v2.tenant_id,
        sp_c_v2.campaign_budget_type,
        sp_c_v2.impressions,
        sp_c_v2.clicks,
        sp_c_v2.attributed_conversions_14d, -- Number of attributed conversion events occurring within 14 days of an ad click.

        -- Amounts
        {# sp_c_v2.currency,
        sp_c_v2.cost,
        sp_c_v2.attributed_sales_14d, -- Total value of sales occurring within 14 days of an ad click. #}

        -- cost_usd
        case
            when sp_c_v2.currency = "USD"
                then sp_c_v2.cost
            else SAFE_DIVIDE(sp_c_v2.cost, fx.rate)
        end as cost_usd,

        -- attributed_sales_14d_usd
        case
            when sp_c_v2.currency = "USD"
                then sp_c_v2.attributed_sales_14d
            else SAFE_DIVIDE(sp_c_v2.attributed_sales_14d, fx.rate)
        end as attributed_sales_14d_usd

    from sb_campaign_v2 as sp_c_v2

    left join exchange_rates as fx
        on
            sp_c_v2.currency = fx.target
            and sp_c_v2.campaign_date = fx.recorded_at

)

select * from convert_sb_campaign_v2_amounts_to_usd
