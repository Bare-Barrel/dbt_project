-- int_kpi_tracker__filter_sb_campaigns.sql kpi_sb_c_04
-- API Data source v3 (2023-09-21 - present)

{{ config(materialized='ephemeral') }}

with

sb_campaigns_with_portfolio_code as (

    select * from {{ ref('int_kpi_tracker__get_sb_portfolio_code') }}

),

filter_by_date as (

    select *
    from sb_campaigns_with_portfolio_code
    where campaign_date >= DATE('2023-09-21')

)

select * from filter_by_date
