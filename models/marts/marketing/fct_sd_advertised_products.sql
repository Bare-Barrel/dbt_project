-- fct_sd_advertised_products.sql

with

sd_advertised_product_usd as (

    select * from {{ ref('int_convert_sd_advertised_product_amounts_to_usd') }}

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
        sd_ap.campaign_date,
        {# created_at, #}
        {# updated_at, #}
        CAST(sd_ap.ad_id as integer) as ad_id,
        CAST(sd_ap.ad_group_id as integer) as ad_group_id,
        sd_ap.ad_group_name,
        CAST(sd_ap.campaign_id as integer) as campaign_id,
        sd_ap.campaign_name,
        {# promoted_sku, #}
        {# bid_optimization, #}

        dpr.product_sk,
        dmp.marketplace_sk,
        dtn.tenant_sk,

        sd_ap.impressions,
        sd_ap.impressions_views,
        sd_ap.impressions_frequency_average,
        sd_ap.clicks,
        sd_ap.units_sold,
        sd_ap.units_sold_clicks,
        {# new_to_brand_units_sold, #}
        {# new_to_brand_units_sold_clicks, #}
        sd_ap.purchases,
        sd_ap.purchases_clicks,
        sd_ap.purchases_promoted_clicks,
        {# new_to_brand_purchases, #}
        {# new_to_brand_purchases_clicks, #}
        {# add_to_cart, #}
        {# add_to_cart_clicks, #}
        {# add_to_cart_views, #}
        sd_ap.detail_page_views,
        sd_ap.detail_page_views_clicks,
        {# new_to_brand_detail_page_views, #}
        {# new_to_brand_ecpd_etail_page_view, #}
        {# new_to_brand_detail_page_view_rate, #}
        {# new_to_brand_detail_page_view_views, #}
        {# new_to_brand_detail_page_view_clicks, #}
        {# video_unmutes, #}
        {# video_complete_views, #}
        {# video_midpoint_views, #}
        {# video_first_quartile_views, #}
        {# video_third_quartile_views, #}
        {# add_to_cart_rate, #}
        {# e_cpa_dd_to_cart, #}
        {# branded_searches, #}
        {# branded_searches_clicks, #}
        {# branded_searches_views, #}
        {# branded_search_rate, #}
        {# e_cpb_rand_search, #}
        {# cumulative_reach, #}
        {# viewability_rate, #}
        {# view_click_through_rate, #}
        sd_ap.cost_usd,
        sd_ap.sales_usd,
        sd_ap.sales_clicks_usd,
        sd_ap.sales_promoted_clicks_usd
    {# new_to_brand_sales_usd, #}
    {# new_to_brand_sales_clicks_usd #}

    from sd_advertised_product_usd as sd_ap

    left join dim_product as dpr
        on sd_ap.promoted_asin = dpr.asin

    left join dim_marketplace as dmp
        on sd_ap.marketplace = dmp.marketplace_name

    left join dim_tenant as dtn
        on sd_ap.tenant_id = dtn.tenant_id

)

select * from add_surrogate_keys
