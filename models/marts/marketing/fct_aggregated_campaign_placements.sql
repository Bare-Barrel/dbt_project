-- fct_aggregated_campaign_placements.sql

with

agg_unioned_campaign_placements as (

    select * from {{ ref('int_aggregate_unioned_campaign_placements') }}

),

dim_marketplace as (

    select * from {{ ref('dim_marketplace') }}

),

dim_product as (

    select * from {{ ref('dim_product') }}

),

dim_tenant as (

    select * from {{ ref('dim_tenant') }}

),

add_surrogate_keys as (

    select
        a_u_cp.record_date,

        dpr.product_sk,
        dmp.marketplace_sk,
        dtn.tenant_sk,

        a_u_cp.campaign_status,
        a_u_cp.placement_classification,
        a_u_cp.ad_type,
        a_u_cp.sb_ad_type,
        a_u_cp.impressions,
        a_u_cp.clicks,
        a_u_cp.units_sold_clicks,
        a_u_cp.purchases_clicks,
        a_u_cp.campaign_budget_amount_usd,
        a_u_cp.cost_usd,
        a_u_cp.sales_clicks_usd

    from agg_unioned_campaign_placements as a_u_cp

    left join dim_product as dpr
        on a_u_cp.asin = dpr.asin

    left join dim_marketplace as dmp
        on a_u_cp.marketplace = dmp.marketplace_name

    left join dim_tenant as dtn
        on a_u_cp.tenant_id = dtn.tenant_id

)

select * from add_surrogate_keys
