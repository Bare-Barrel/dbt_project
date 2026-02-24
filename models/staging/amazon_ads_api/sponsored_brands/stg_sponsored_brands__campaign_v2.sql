-- stg_sponsored_brands__campaign_v2.sql

with

sb_campaign_v2 as (

    select * from {{ source('sponsored_brands', 'campaign_v2') }}

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
        currency,
        campaign_budget_type,
        impressions,
        clicks,
        cost,
        vtr,
        vctr,
        dpv_14d,
        video_unmutes,
        campaign_budget,
        attributed_sales_14d,
        attributed_conversions_14d,
        attributed_conversions_14d_same_sku,
        attributed_orders_new_to_brand_14d,
        attributed_orders_new_to_brand_percentage_14d,
        attributed_order_rate_new_to_brand_14d,
        attributed_units_ordered_new_to_brand_14d,
        attributed_units_ordered_new_to_brand_percentage_14d,
        attributed_sales_14d_same_sku,
        attributed_sales_new_to_brand_14d,
        attributed_sales_new_to_brand_percentage_14d,
        attributed_branded_searches_14d,
        attributed_detail_page_views_clicks_14d,
        video_5_second_views,
        video_5_second_view_rate,
        video_complete_views,
        video_midpoint_views,
        viewable_impressions,
        video_first_quartile_views,
        video_third_quartile_views,
        top_of_search_impression_share

    from sb_campaign_v2

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
        currency,
        campaign_budget_type,

        -- numerics
        impressions,
        clicks,
        cost,
        vtr,
        vctr,
        dpv_14d,
        video_unmutes,
        campaign_budget,
        attributed_sales_14d,
        attributed_conversions_14d,
        attributed_conversions_14d_same_sku,
        attributed_orders_new_to_brand_14d,
        attributed_orders_new_to_brand_percentage_14d,
        attributed_order_rate_new_to_brand_14d,
        attributed_units_ordered_new_to_brand_14d,
        attributed_units_ordered_new_to_brand_percentage_14d,
        attributed_sales_14d_same_sku,
        attributed_sales_new_to_brand_14d,
        attributed_sales_new_to_brand_percentage_14d,
        attributed_branded_searches_14d,
        attributed_detail_page_views_clicks_14d,
        video_5_second_views,
        video_5_second_view_rate,
        video_complete_views,
        video_midpoint_views,
        viewable_impressions,
        video_first_quartile_views,
        video_third_quartile_views,
        top_of_search_impression_share,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
