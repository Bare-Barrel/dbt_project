-- fct_campaigns.sql

with

campaigns as (

    select * from {{ ref('int_union_campaigns') }}

),

dim_campaigns as (

    select * from {{ ref('dim_campaigns') }}

),

dim_marketplaces as (

    select * from {{ ref('dim_marketplaces') }}

),

dim_tenants as (

    select * from {{ ref('dim_tenants') }}

),

dim_portfolios as (

    select * from {{ ref('dim_portfolios') }}

),

dim_ad_types as (

    select * from {{ ref('dim_ad_types') }}

),

dim_ppc_products as (

    select * from {{ ref('dim_ppc_products') }}

),

build_c_fact_table as (

    select
        c.record_date,

        dat.ad_type_sk,
        dcam.campaign_sk,
        dmp.marketplace_sk,
        dpo.portfolio_sk,
        dpr.ppc_product_sk,
        dtn.tenant_sk,

        c.impressions,
        c.clicks,
        c.units_sold_clicks,
        c.sales_clicks_usd,
        c.campaign_budget_amount_usd,
        c.cost_usd,
        c.cost_per_click_usd,
        c.click_through_rate,
        c.conversion_rate,
        c.top_of_search_impression_share

    from campaigns as c

    left join dim_campaigns as dcam
        on
            c.campaign_id = dcam.campaign_id
            and c.record_date >= dcam.start_date
            and c.record_date <= dcam.end_date

    left join dim_marketplaces as dmp
        on c.marketplace = dmp.marketplace_name

    left join dim_tenants as dtn
        on c.tenant_id = dtn.tenant_id

    left join dim_portfolios as dpo
        on c.portfolio_id = dpo.portfolio_id

    left join dim_ad_types as dat
        on c.ad_type = dat.ad_type

    left join dim_ppc_products as dpr
        on
            c.parent_code = dpr.parent_code
            and c.portfolio_code = dpr.portfolio_code
            and c.product_code = dpr.product_code
            and c.product_color = dpr.product_color
            and c.product_pack_size = dpr.product_pack_size

)

select * from build_c_fact_table
