-- fct_campaigns_and_placements.sql

with

all_campaigns as (

    select * from {{ ref('int_union_all_campaigns') }}

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

build_ac_fact_table as (

    select
        ac.record_date,

        dcam.campaign_sk,
        dpo.portfolio_sk,
        dmp.marketplace_sk,
        dpl.placement_sk,
        dtn.tenant_sk,
        dadt.ad_type_sk,
        dpr.ppc_product_sk,

        ac.impressions,
        ac.clicks,
        ac.units_sold_clicks,
        ac.sales_clicks_usd,
        ac.campaign_budget_amount_usd,
        ac.cost_usd,
        ac.cost_per_click_usd,
        ac.click_through_rate,
        ac.conversion_rate,
        ac.top_of_search_impression_share

    from all_campaigns as ac

    left join dim_campaigns as dcam
        on
            ac.campaign_id = dcam.campaign_id
            and ac.record_date >= dcam.start_date
            and ac.record_date <= dcam.end_date

    left join dim_portfolios as dpo
        on ac.portfolio_id = dpo.portfolio_id

    left join dim_marketplaces as dmp
        on ac.marketplace = dmp.marketplace_name

    left join dim_placements as dpl
        on ac.placement_classification = dpl.placement_classification

    left join dim_tenants as dtn
        on ac.tenant_id = dtn.tenant_id

    left join dim_ad_types as dadt
        on ac.ad_type = dadt.ad_type

    left join dim_ppc_products as dpr
        on
            ac.parent_code = dpr.parent_code
            and ac.portfolio_code = dpr.portfolio_code
            and ac.product_code = dpr.product_code
            and ac.product_color = dpr.product_color
            and ac.product_pack_size = dpr.product_pack_size

)

select * from build_ac_fact_table
