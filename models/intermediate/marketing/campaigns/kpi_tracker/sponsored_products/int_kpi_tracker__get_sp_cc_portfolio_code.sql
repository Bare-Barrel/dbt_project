-- int_kpi_tracker__get_sp_cc_portfolio_code.sql kpi_sp_cc_03
-- Advertising reports data source. (06-27-2022 to 2024-04-01)

{{ config(materialized='ephemeral') }}

with

filtered_sp_campaign_console as (

    select * from {{ ref('int_kpi_tracker__filter_sp_cc') }}

),

get_sp_cc_portfolio_code as (

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
        last_year_cost_per_click_usd,

        -- Portfolio Code
        case
            when tenant_id = 1
                then
                    case
                        when REGEXP_CONTAINS(portfolio_name, r"-L1$")
                            then REGEXP_REPLACE(portfolio_name, r"-L1$", "")
                        else portfolio_name
                    end
        end as portfolio_code

    from filtered_sp_campaign_console

)

select * from get_sp_cc_portfolio_code
