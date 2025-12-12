-- fct_campaign_placements.sql

with

campaign_placements as (

    select * from {{ ref('int_union_campaign_placements') }}

),

dim_campaigns as (

    select * from {{ ref('dim_campaigns') }}

),

dim_portfolios as (

    select * from {{ ref('dim_portfolios') }}

),

dim_marketplaces as (

    select * from {{ ref('dim_marketplaces') }}

),

dim_placements as (

    select * from {{ ref('dim_placements') }}

),

dim_tenants as (

    select * from {{ ref('dim_tenants') }}

),

dim_ad_types as (

    select * from {{ ref('dim_ad_types') }}

),

dim_ppc_products as (

    select * from {{ ref('dim_ppc_products') }}

),

build_cp_fact_table as (

    select
        cp.record_date,

        dcam.campaign_sk,
        dpo.portfolio_sk,
        dmp.marketplace_sk,
        dpl.placement_sk,
        dtn.tenant_sk,
        dadt.ad_type_sk,
        dpr.ppc_product_sk,

        cp.impressions,
        cp.clicks,
        cp.units_sold_clicks,
        cp.sales_clicks_usd,
        cp.purchases_clicks,
        cp.tenant_id,
        cp.campaign_budget_amount_usd,
        cp.cost_usd,
        cp.cost_per_click_usd,
        cp.click_through_rate,
        cp.conversion_rate

    from campaign_placements as cp

    left join dim_campaigns as dcam
        on
            cp.campaign_id = dcam.campaign_id
            and cp.record_date >= dcam.start_date
            and cp.record_date <= dcam.end_date

    left join dim_portfolios as dpo
        on cp.portfolio_id = dpo.portfolio_id

    left join dim_marketplaces as dmp
        on cp.marketplace = dmp.marketplace_name

    left join dim_placements as dpl
        on cp.placement_classification = dpl.placement_classification

    left join dim_tenants as dtn
        on cp.tenant_id = dtn.tenant_id

    left join dim_ad_types as dadt
        on cp.ad_type = dadt.ad_type

    left join dim_ppc_products as dpr
        on
            cp.parent_code = dpr.parent_code
            and cp.portfolio_code = dpr.portfolio_code
            and cp.product_code = dpr.product_code
            and cp.product_color = dpr.product_color
            and cp.product_pack_size = dpr.product_pack_size

)

select * from build_cp_fact_table
