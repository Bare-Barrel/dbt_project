-- stg_sponsored_brands__campaign.sql

with

sb_campaign as (

    select * from {{ source('sponsored_brands', 'campaign') }}

),

rename_fields as (

    select
        date as campaign_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        marketplace,
        tenant_id,
        cost_type,
        campaign_budget_currency_code,
        campaign_budget_type,
        impressions,
        clicks,
        cost,
        sales,
        sales_clicks,
        sales_promoted,
        units_sold,
        units_sold_clicks,
        purchases,
        purchases_clicks,
        purchases_promoted,
        add_to_cart,
        add_to_cart_clicks,
        add_to_cart_rate,
        video_unmutes,
        branded_searches,
        branded_searches_clicks,
        e_cpa_dd_to_cart,
        viewability_rate,
        detail_page_views,
        detail_page_views_clicks,
        new_to_brand_units_sold,
        new_to_brand_units_sold_clicks,
        new_to_brand_units_sold_percentage,
        new_to_brand_sales,
        new_to_brand_sales_clicks,
        new_to_brand_sales_percentage,
        new_to_brand_purchases,
        new_to_brand_purchases_clicks,
        new_to_brand_purchases_rate,
        new_to_brand_purchases_percentage,
        new_to_brand_detail_page_views,
        new_to_brand_detail_page_views_clicks,
        new_to_brand_detail_page_view_rate,
        new_to_brand_ecpd_etail_page_view,
        video_5_second_views,
        video_5_second_view_rate,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,
        viewable_impressions,
        campaign_budget_amount,
        view_click_through_rate,
        top_of_search_impression_share

    from sb_campaign

),

cast_data_types as (

    select
        -- ids
        CAST(campaign_id as string) as campaign_id,
        tenant_id,

        -- strings
        campaign_name,
        campaign_status,
        marketplace,
        cost_type,
        campaign_budget_currency_code,
        campaign_budget_type,

        -- numerics
        impressions,
        clicks,
        cost,
        sales,
        sales_clicks,
        sales_promoted,
        units_sold,
        units_sold_clicks,
        purchases,
        purchases_clicks,
        purchases_promoted,
        add_to_cart,
        add_to_cart_clicks,
        add_to_cart_rate,
        video_unmutes,
        branded_searches,
        branded_searches_clicks,
        e_cpa_dd_to_cart,
        viewability_rate,
        detail_page_views,
        detail_page_views_clicks,
        new_to_brand_units_sold,
        new_to_brand_units_sold_clicks,
        new_to_brand_units_sold_percentage,
        new_to_brand_sales,
        new_to_brand_sales_clicks,
        new_to_brand_sales_percentage,
        new_to_brand_purchases,
        new_to_brand_purchases_clicks,
        new_to_brand_purchases_rate,
        new_to_brand_purchases_percentage,
        new_to_brand_detail_page_views,
        new_to_brand_detail_page_views_clicks,
        new_to_brand_detail_page_view_rate,
        new_to_brand_ecpd_etail_page_view,
        video_5_second_views,
        video_5_second_view_rate,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,
        viewable_impressions,
        campaign_budget_amount,
        view_click_through_rate,
        top_of_search_impression_share,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
