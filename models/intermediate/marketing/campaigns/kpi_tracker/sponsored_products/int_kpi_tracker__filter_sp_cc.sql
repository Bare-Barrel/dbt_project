-- int_kpi_tracker__filter_sp_cc.sql kpi_sp_cc_02
-- Advertising reports data source. (06-27-2022 to 2024-04-01)

{{ config(materialized='ephemeral') }}

with

sp_campaign_console_usd as (

    select * from {{ ref('int_convert_sp_cc_amounts_to_usd') }}

),

filter_by_date as (

    select *
    from sp_campaign_console_usd
    where campaign_date < DATE('2023-04-02')

),

filter_out_zero_impressions as (

    select
        campaign_date,
        created_at,
        updated_at,
        tenant_id,
        campaign_name,
        campaign_type,
        campaign_status,
        portfolio_name,
        marketplace,
        country,
        targeting_type,
        bidding_strategy,
        impressions,
        clicks,
        _7_day_total_orders,
        click_thru_rate,
        last_year_impressions,
        last_year_clicks,
        total_advertising_cost_of_sales,
        total_return_on_advertising_spend,
        spend_usd,
        budget_usd,
        _7_day_total_sales_usd,
        cost_per_click_usd,
        last_year_spend_usd,
        last_year_cost_per_click_usd

    from filter_by_date

    where impressions > 0

)

select * from filter_out_zero_impressions
