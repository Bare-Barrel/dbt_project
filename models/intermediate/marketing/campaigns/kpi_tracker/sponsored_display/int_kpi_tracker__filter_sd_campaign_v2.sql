-- int_kpi_tracker__filter_sd_campaign_v2.sql kpi_sd_c_v2_04
-- API v2 (2023-06-11 - 2025-01-10)

{{ config(materialized='ephemeral') }}

with

sd_campaign_v2_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sd_campaign_v2_portfolio_code') }}

),

filter_by_date as (

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        portfolio_code,
        marketplace,
        tenant_id,
        cost_type,
        impressions,
        clicks,
        cost_usd,
        attributed_units_ordered_14d,
        attributed_conversions_14d,
        attributed_sales_14d_usd

    from sd_campaign_v2_with_portfolio_code

    where
        campaign_date >= DATE('2023-06-11')
        and campaign_date <= DATE('2025-01-10')

)

select * from filter_by_date
