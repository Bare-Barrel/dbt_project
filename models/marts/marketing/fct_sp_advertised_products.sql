-- fct_sp_advertised_products.sql

with

sp_advertised_product_usd as (

    select * from {{ ref('int_convert_sp_advertised_product_amounts_to_usd') }}

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

add_surrogate_keys as ( -- Cast IDs to integers to optimize import compression in Power BI

    select
        sp_ap.campaign_date,
        {# created_at,
        updated_at, #}
        CAST(sp_ap.ad_id as integer) as ad_id,
        CAST(sp_ap.ad_group_id as integer) as ad_group_id,
        CAST(sp_ap.campaign_id as integer) as campaign_id,
        CAST(sp_ap.portfolio_id as integer) as portfolio_id,
        sp_ap.ad_group_name,
        sp_ap.campaign_name,
        sp_ap.campaign_status,
        {# advertised_sku, #}
        {# campaign_budget_type, #}
        {# campaign_budget_currency_code, #}

        dpr.product_sk,
        dmp.marketplace_sk,
        dtn.tenant_sk,

        sp_ap.impressions,
        sp_ap.cost_usd,
        sp_ap.spend_usd,
        sp_ap.clicks,
        sp_ap.sales_14d_usd,
        sp_ap.purchases_14d,
        sp_ap.purchases_same_sku_14d,
        sp_ap.acos_clicks_14d,
        sp_ap.cost_per_click_usd,
        sp_ap.roas_clicks_14d,
        sp_ap.click_through_rate,
        sp_ap.units_sold_clicks_14d,
        sp_ap.units_sold_same_sku_14d,
        sp_ap.campaign_budget_amount_usd,
        sp_ap.attributed_sales_same_sku_14d_usd,
        sp_ap.kindle_edition_normalized_pages_read_14d,
        sp_ap.kindle_edition_normalized_pages_royalties_14d

    from sp_advertised_product_usd as sp_ap

    left join dim_product as dpr
        on sp_ap.advertised_asin = dpr.asin

    left join dim_marketplace as dmp
        on sp_ap.marketplace = dmp.marketplace_name

    left join dim_tenant as dtn
        on sp_ap.tenant_id = dtn.tenant_id

)

select * from add_surrogate_keys
