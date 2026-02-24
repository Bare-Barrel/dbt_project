-- int_kpi_tracker__filter_sb_campaign_v2.sql kpi_sb_c_v2_04
-- API Data source v2 (2023-06-11 - 2023-09-20)

{{ config(materialized='ephemeral') }}

with

sb_campaign_v2_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sb_campaign_v2_portfolio_code') }}

),

filter_by_date_and_status as (

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
        campaign_budget_type,
        impressions,
        clicks,
        attributed_conversions_14d,
        cost_usd,
        attributed_sales_14d_usd

    from sb_campaign_v2_with_portfolio_code

    where
        campaign_status in ('enabled', 'paused')
        and campaign_date > DATE('2023-06-10')
        and campaign_date <= DATE('2023-09-20')

)

select * from filter_by_date_and_status
