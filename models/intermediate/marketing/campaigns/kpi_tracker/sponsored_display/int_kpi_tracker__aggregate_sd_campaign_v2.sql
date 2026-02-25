-- int_kpi_tracker__aggregate_sd_campaign_v2.sql kpi_sd_c_v2_05
-- API v2 (2023-06-11 - 2025-01-10)

{{ config(materialized='view') }}

with

filtered_sd_campaign_v2 as (

    select * from {{ ref('int_kpi_tracker__filter_sd_campaign_v2') }}

),

aggregate_sd_campaign_v2 as (

    select
        campaign_date,
        portfolio_code,
        marketplace,
        tenant_id,

        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(cost_usd) as total_cost_usd,
        SUM(attributed_units_ordered_14d) as total_attributed_units_ordered_14d,
        SUM(attributed_conversions_14d) as total_attributed_conversions_14d,
        SUM(attributed_sales_14d_usd) as total_attributed_sales_14d_usd

    from filtered_sd_campaign_v2

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, tenant_id asc, marketplace desc, portfolio_code asc

)

select * from aggregate_sd_campaign_v2
