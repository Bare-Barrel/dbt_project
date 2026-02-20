-- int_kpi_tracker__aggregate_sp_cc.sql kpi_sp_cc_04
-- Advertising reports data source. (06-27-2022 to 2024-04-01)

{{ config(materialized='view') }}

with

sp_campaign_console_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sp_cc_portfolio_code') }}

),

aggregate_sp_cc as (

    select
        campaign_date,
        tenant_id,
        marketplace,
        portfolio_code,

        -- Amounts
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(_7_day_total_orders) as _7_day_total_orders,
        SUM(click_thru_rate) as total_click_thru_rate, -- check before using!!
        SUM(last_year_impressions) as total_last_year_impressions,
        SUM(last_year_clicks) as total_last_year_clicks,
        SUM(total_advertising_cost_of_sales) as total_advertising_cost_of_sales,  -- check before using!!
        SUM(total_return_on_advertising_spend) as total_return_on_advertising_spend,  -- check before using!!
        SUM(spend_usd) as total_spend_usd,
        SUM(budget_usd) as total_budget_usd,
        SUM(_7_day_total_sales_usd) as _7_day_total_sales_usd,
        SUM(cost_per_click_usd) as total_cost_per_click_usd,
        SUM(last_year_spend_usd) as total_last_year_spend_usd,
        SUM(last_year_cost_per_click_usd) as total_last_year_cost_per_click_usd

    from sp_campaign_console_with_portfolio_code

    group by campaign_date, tenant_id, marketplace, portfolio_code

    order by campaign_date desc, marketplace desc, portfolio_code asc

)

select * from aggregate_sp_cc
