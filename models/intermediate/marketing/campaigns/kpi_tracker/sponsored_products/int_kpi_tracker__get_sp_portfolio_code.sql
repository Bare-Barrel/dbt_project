-- int_kpi_tracker__get_sp_portfolio_code.sql kpi_sp_c_03
-- API data source. (04-02-2023 to present)

{{ config(materialized='ephemeral') }}

with

sp_campaigns_with_portfolio as (

    select * from {{ ref('int_get_sp_portfolio') }}

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
        impressions,
        clicks,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        click_through_rate,
        top_of_search_impression_share,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,

        -- Portfolio Code - should match portfolio_code in dim_products
        case
            when tenant_id = 1
                then
                    case
                        when REGEXP_CONTAINS(portfolio_name, r"-L1$")
                            then REGEXP_REPLACE(portfolio_name, r"-L1$", "")
                        else portfolio_name
                    end
            when tenant_id = 2
                then
                    case
                        when REGEXP_CONTAINS(portfolio_name, "MIX")
                            then portfolio_name
                        when REGEXP_CONTAINS(portfolio_name, "CALF")
                            then "R_CALF-SLEEV"
                        when REGEXP_CONTAINS(portfolio_name, "COMP")
                            then "R_COMP-SOCKS"
                        when REGEXP_CONTAINS(portfolio_name, "ELBO")
                            then "R_ELBO-SLE"
                        when REGEXP_CONTAINS(portfolio_name, "GRIP")
                            then "R_GRIP-SOCKS"
                        when REGEXP_CONTAINS(portfolio_name, "HIK")
                            then "R_HIKE-SOC"
                        when REGEXP_CONTAINS(portfolio_name, "KNEE")
                            then "R_KNEE-SLE"
                        when REGEXP_CONTAINS(portfolio_name, "PF")
                            then "R_PF-SOCKS"
                    end
        end as portfolio_code

    from sp_campaigns_with_portfolio

)

select * from get_portfolio_code
