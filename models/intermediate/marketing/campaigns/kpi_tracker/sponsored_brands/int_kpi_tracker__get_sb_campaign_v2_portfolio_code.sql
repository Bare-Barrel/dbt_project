-- int_kpi_tracker__get_sb_campaign_v2_portfolio_code.sql kpi_sb_c_v2_03
-- API Data source v2 (2023-06-11 - 2023-09-20)

{{ config(materialized='ephemeral') }}

with

sb_campaigns_v2_with_portfolio as (

    select * from {{ ref('int_get_sb_campaign_v2_portfolio') }}

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
        campaign_budget_type,
        impressions,
        clicks,
        attributed_conversions_14d,
        cost_usd,
        attributed_sales_14d_usd,

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

    from sb_campaigns_v2_with_portfolio

)

select * from get_portfolio_code
