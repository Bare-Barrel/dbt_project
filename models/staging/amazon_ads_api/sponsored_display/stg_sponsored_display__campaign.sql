-- stg_sponsored_display__campaign.sql

with

sd_campaign as (

    select * from {{ source('sponsored_display', 'campaign') }}

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
        impressions,
        clicks,
        cost,
        sales,
        sales_clicks,
        units_sold,
        units_sold_clicks,
        purchases,
        purchases_clicks,
        purchases_promoted_clicks,
        campaign_budget_amount,
        add_to_cart,
        add_to_cart_clicks,
        add_to_cart_views,
        add_to_cart_rate,
        e_cpa_dd_to_cart,
        e_cpb_rand_search,
        impressions_views,
        impressions_frequency_average,
        video_unmutes,
        branded_searches,
        branded_searches_clicks,
        branded_searches_views,
        branded_search_rate,
        cumulative_reach,
        viewability_rate,
        detail_page_views,
        detail_page_views_clicks,
        sales_promoted_clicks,
        new_to_brand_units_sold,
        new_to_brand_units_sold_clicks,
        new_to_brand_sales,
        new_to_brand_sales_clicks,
        new_to_brand_purchases,
        new_to_brand_purchases_clicks,
        new_to_brand_detail_page_views,
        new_to_brand_detail_page_view_clicks,
        new_to_brand_detail_page_view_views,
        new_to_brand_detail_page_view_rate,
        new_to_brand_ecpd_etail_page_view,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,
        view_click_through_rate

    from sd_campaign

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

        -- numerics
        impressions,
        clicks,
        cost,
        sales,
        sales_clicks,
        units_sold,
        units_sold_clicks,
        purchases,
        purchases_clicks,
        purchases_promoted_clicks,
        campaign_budget_amount,
        add_to_cart,
        add_to_cart_clicks,
        add_to_cart_views,
        add_to_cart_rate,
        e_cpa_dd_to_cart,
        e_cpb_rand_search,
        impressions_views,
        impressions_frequency_average,
        video_unmutes,
        branded_searches,
        branded_searches_clicks,
        branded_searches_views,
        branded_search_rate,
        cumulative_reach,
        viewability_rate,
        detail_page_views,
        detail_page_views_clicks,
        sales_promoted_clicks,
        new_to_brand_units_sold,
        new_to_brand_units_sold_clicks,
        new_to_brand_sales,
        new_to_brand_sales_clicks,
        new_to_brand_purchases,
        new_to_brand_purchases_clicks,
        new_to_brand_detail_page_views,
        new_to_brand_detail_page_view_clicks,
        new_to_brand_detail_page_view_views,
        new_to_brand_detail_page_view_rate,
        new_to_brand_ecpd_etail_page_view,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,
        view_click_through_rate,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
