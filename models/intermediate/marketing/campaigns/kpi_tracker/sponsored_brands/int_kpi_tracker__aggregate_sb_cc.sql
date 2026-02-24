-- int_kpi_tracker__aggregate_sb_cc.sql kpi_sb_cc_03
-- Advertising reports from advertising console (2022-07-10 - 2023-06-10)

{{ config(materialized='view') }}

with

filtered_sb_campaign_console as (

    select * from {{ ref('int_kpi_tracker__filter_sb_cc') }}

),

aggregate_sb_campaign_console as (

    select
        campaign_date,
        tenant_id,
        marketplace,
        portfolio_code,

        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(spend_usd) as total_spend_usd,
        SUM(_14_day_total_units) as total_14_day_total_units,
        SUM(_14_day_total_orders) as total_14_day_total_orders,
        SUM(_14_day_total_sales_usd) as total_14_day_total_sales_usd

    from filtered_sb_campaign_console

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, marketplace desc, portfolio_code asc

)

select * from aggregate_sb_campaign_console
