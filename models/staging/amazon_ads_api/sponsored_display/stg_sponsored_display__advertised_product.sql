-- stg_sponsored_display__advertised_product.sql

with

sd_advertised_product as (

    select * from {{ source('sponsored_display', 'advertised_product') }}

),

rename_fields as (

    select
        date as campaign_date,
        created_at,
        updated_at,
        ad_id,
        ad_group_id,
        ad_group_name,
        campaign_id,
        campaign_name,
        promoted_sku,
        promoted_asin,
        bid_optimization,
        marketplace,
        tenant_id,
        campaign_budget_currency_code,
        impressions,
        impressions_views,
        impressions_frequency_average,
        clicks,
        cost,
        units_sold,
        units_sold_clicks,
        new_to_brand_units_sold,
        new_to_brand_units_sold_clicks,
        sales,
        sales_clicks,
        sales_promoted_clicks,
        new_to_brand_sales,
        new_to_brand_sales_clicks,
        purchases,
        purchases_clicks,
        purchases_promoted_clicks,
        new_to_brand_purchases,
        new_to_brand_purchases_clicks,
        add_to_cart,
        add_to_cart_clicks,
        add_to_cart_views,
        detail_page_views,
        detail_page_views_clicks,
        new_to_brand_detail_page_views,
        new_to_brand_ecpd_etail_page_view,
        new_to_brand_detail_page_view_rate,
        new_to_brand_detail_page_view_views,
        new_to_brand_detail_page_view_clicks,
        video_unmutes,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,
        add_to_cart_rate,
        e_cpa_dd_to_cart,
        branded_searches,
        branded_searches_clicks,
        branded_searches_views,
        branded_search_rate,
        e_cpb_rand_search,
        cumulative_reach,
        viewability_rate,
        view_click_through_rate

    from sd_advertised_product

),

cast_data_types as (

    select
        -- ids
        CAST(ad_id as string) as ad_id,
        CAST(ad_group_id as string) as ad_group_id,
        CAST(campaign_id as string) as campaign_id,
        tenant_id,

        -- strings
        ad_group_name,
        campaign_name,
        promoted_sku,
        promoted_asin,
        bid_optimization,
        marketplace,
        campaign_budget_currency_code,

        -- numerics
        impressions,
        impressions_views,
        impressions_frequency_average,
        clicks,
        cost,
        units_sold,
        units_sold_clicks,
        new_to_brand_units_sold,
        new_to_brand_units_sold_clicks,
        sales,
        sales_clicks,
        sales_promoted_clicks,
        new_to_brand_sales,
        new_to_brand_sales_clicks,
        purchases,
        purchases_clicks,
        purchases_promoted_clicks,
        new_to_brand_purchases,
        new_to_brand_purchases_clicks,
        add_to_cart,
        add_to_cart_clicks,
        add_to_cart_views,
        detail_page_views,
        detail_page_views_clicks,
        new_to_brand_detail_page_views,
        new_to_brand_ecpd_etail_page_view,
        new_to_brand_detail_page_view_rate,
        new_to_brand_detail_page_view_views,
        new_to_brand_detail_page_view_clicks,
        video_unmutes,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,
        add_to_cart_rate,
        e_cpa_dd_to_cart,
        branded_searches,
        branded_searches_clicks,
        branded_searches_views,
        branded_search_rate,
        e_cpb_rand_search,
        cumulative_reach,
        viewability_rate,
        view_click_through_rate,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
