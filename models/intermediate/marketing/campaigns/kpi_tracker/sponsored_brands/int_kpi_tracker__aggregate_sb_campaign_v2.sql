-- int_kpi_tracker__aggregate_sb_campaign_v2.sql kpi_sb_c_v2_05

{{ config(materialized='view') }}

with

filtered_sb_campaign_v2 as (

    select * from {{ ref('int_kpi_tracker__filter_sb_campaign_v2') }}

),

aggregate_sb_campaign_v2 as (

    select
        campaign_date,
        tenant_id,
        marketplace,
        portfolio_code,

        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(attributed_conversions_14d) as total_attributed_conversions_14d,
        SUM(cost_usd) as total_cost_usd,
        SUM(attributed_sales_14d_usd) as total_attributed_sales_14d_usd

    from filtered_sb_campaign_v2

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, tenant_id asc, marketplace desc, portfolio_code asc

)

select * from aggregate_sb_campaign_v2
