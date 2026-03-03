-- obt_sd_advertised_products.sql

with

sd_advertised_product_usd as (

    select * from {{ ref('int_convert_sd_advertised_product_amounts_to_usd') }}

),

cast_ids_to_integer as ( -- Optimize import compression in Power BI

    select
        campaign_date,
        {# created_at, #}
        {# updated_at, #}
        CAST(ad_id as integer) as ad_id,
        CAST(ad_group_id as integer) as ad_group_id,
        ad_group_name,
        CAST(campaign_id as integer) as campaign_id,
        campaign_name,
        {# promoted_sku, #}
        promoted_asin,
        {# bid_optimization, #}
        marketplace,
        tenant_id,
        impressions,
        impressions_views,
        impressions_frequency_average,
        clicks,
        units_sold,
        units_sold_clicks,
        {# new_to_brand_units_sold, #}
        {# new_to_brand_units_sold_clicks, #}
        purchases,
        purchases_clicks,
        purchases_promoted_clicks,
        {# new_to_brand_purchases, #}
        {# new_to_brand_purchases_clicks, #}
        {# add_to_cart, #}
        {# add_to_cart_clicks, #}
        {# add_to_cart_views, #}
        detail_page_views,
        detail_page_views_clicks,
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
        cost_usd,
        sales_usd,
        sales_clicks_usd,
        sales_promoted_clicks_usd
    {# new_to_brand_sales_usd, #}
    {# new_to_brand_sales_clicks_usd #}

    from sd_advertised_product_usd

)

select * from cast_ids_to_integer
