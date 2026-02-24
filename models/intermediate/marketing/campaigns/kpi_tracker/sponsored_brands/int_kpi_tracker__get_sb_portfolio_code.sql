-- int_kpi_tracker__get_sb_portfolio_code.sql kpi_sb_c_03
-- API Data source v3 (2023-09-21 - present)

{{ config(materialized='ephemeral') }}

with

sb_campaigns_with_portfolio as (

    select * from {{ ref('int_get_sb_portfolio') }}

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
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        top_of_search_impression_share,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,

        -- Portfolio Code - should match portfolio_code in dim_products
        case
            when tenant_id = 1
                then portfolio_name
            when tenant_id = 2
                then
                    case
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

    from sb_campaigns_with_portfolio

)

select * from get_portfolio_code
