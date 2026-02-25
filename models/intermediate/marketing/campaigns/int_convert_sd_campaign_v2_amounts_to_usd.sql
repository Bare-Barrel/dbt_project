-- int_convert_sd_campaign_v2_amounts_to_usd.sql sd_c_v2_01
-- API v2 (2023-06-11 - 2025-01-10)

{{ config(materialized='ephemeral') }}

with

sd_campaign_v2 as (

    select * from {{ ref('stg_sponsored_display__campaign_v2') }}

),

rename_sd_v2_amounts_to_usd as (

    select
        sd_c_v2.campaign_date,
        sd_c_v2.created_at,
        sd_c_v2.updated_at,
        sd_c_v2.campaign_id,
        sd_c_v2.campaign_name,
        sd_c_v2.campaign_status,
        sd_c_v2.marketplace,
        sd_c_v2.tenant_id,
        sd_c_v2.cost_type,
        sd_c_v2.impressions,
        sd_c_v2.clicks,
        sd_c_v2.attributed_units_ordered_14d,
        sd_c_v2.attributed_conversions_14d,

        -- Amounts  -- all amounts are already in USD
        sd_c_v2.cost as cost_usd,
        sd_c_v2.attributed_sales_14d as attributed_sales_14d_usd

    from sd_campaign_v2 as sd_c_v2

)

select * from rename_sd_v2_amounts_to_usd
