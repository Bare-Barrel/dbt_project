-- fct_sp_campaigns.sql

with

unioned_sp_campaigns as (

    select * from {{ ref('int_kpi_tracker__union_all_sp_campaigns') }}

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
        u_sp_c.campaign_date,

        dpc.portfolio_code_sk,
        dmp.marketplace_sk,
        dtn.tenant_sk,

        u_sp_c.total_impressions,
        u_sp_c.total_clicks,
        u_sp_c.total_cost_usd,
        u_sp_c.total_units_sold_clicks_7d,
        u_sp_c.total_sales_7d_usd

    from unioned_sp_campaigns as u_sp_c

    left join dim_portfolio_code as dpc
        on u_sp_c.portfolio_code = dpc.portfolio_code

    left join dim_marketplace as dmp
        on u_sp_c.marketplace = dmp.marketplace_name

    left join dim_tenant as dtn
        on u_sp_c.tenant_id = dtn.tenant_id

)

select * from add_surrogate_keys
