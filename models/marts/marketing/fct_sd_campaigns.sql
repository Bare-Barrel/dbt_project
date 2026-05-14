-- fct_sd_campaigns.sql

with

unioned_sd_campaigns as (

    select * from {{ ref('int_kpi_tracker__union_all_sd_campaigns') }}

),

dim_marketplace as (

    select * from {{ ref('dim_marketplace') }}

),

dim_portfolio_code as (

    select * from {{ ref('dim_portfolio_code') }}

),

dim_tenant as (

    select * from {{ ref('dim_tenant') }}

),

add_surrogate_keys as (

    select
        u_sd_c.campaign_date,

        dpc.portfolio_code_sk,
        dmp.marketplace_sk,
        dtn.tenant_sk,

        u_sd_c.total_impressions,
        u_sd_c.total_clicks,
        u_sd_c.total_cost_usd,
        u_sd_c.total_units_sold_clicks,
        u_sd_c.total_purchases_clicks,
        u_sd_c.total_sales_clicks_usd

    from unioned_sd_campaigns as u_sd_c

    left join dim_portfolio_code as dpc
        on u_sd_c.portfolio_code = dpc.portfolio_code

    left join dim_marketplace as dmp
        on u_sd_c.marketplace = dmp.marketplace_name

    left join dim_tenant as dtn
        on u_sd_c.tenant_id = dtn.tenant_id

)

select * from add_surrogate_keys
