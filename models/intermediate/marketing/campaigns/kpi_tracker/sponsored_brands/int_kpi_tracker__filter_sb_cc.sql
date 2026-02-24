-- int_kpi_tracker__filter_sb_cc.sql kpi_sb_cc_02
-- Advertising reports from advertising console (2022-07-10 - 2023-06-10)

{{ config(materialized='ephemeral') }}

with

sb_campaign_console_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sb_cc_portfolio_code') }}

),

filter_by_date as (

    select
        campaign_date,
        created_at,
        updated_at,
        campaign_name,
        portfolio_name,
        portfolio_code,
        marketplace,
        tenant_id,
        impressions,
        clicks,
        spend_usd,
        _14_day_total_units,
        _14_day_total_orders,
        _14_day_total_sales_usd

    from sb_campaign_console_with_portfolio_code

    where campaign_date <= DATE('2023-06-10')

)

select * from filter_by_date
