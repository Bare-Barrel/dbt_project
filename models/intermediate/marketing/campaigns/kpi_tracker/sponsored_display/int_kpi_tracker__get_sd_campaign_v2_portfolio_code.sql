-- int_kpi_tracker__get_sd_campaign_v2_portfolio_code.sql kpi_sd_c_v2_03
-- API v2 (2023-06-11 - 2025-01-10)

{{ config(materialized='ephemeral') }}

with

sd_campaign_v2_with_portfolio as (

    select * from {{ ref('int_get_sd_campaign_v2_portfolio') }}

),

get_portfolio_code as (

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        marketplace,
        tenant_id,
        cost_type,
        impressions,
        clicks,
        cost_usd,
        attributed_units_ordered_14d,
        attributed_conversions_14d,
        attributed_sales_14d_usd,

        -- Portfolio Code
        portfolio_name as portfolio_code

    from sd_campaign_v2_with_portfolio

)

select * from get_portfolio_code
