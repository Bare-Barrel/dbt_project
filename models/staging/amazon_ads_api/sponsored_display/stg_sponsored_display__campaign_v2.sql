-- stg_sponsored_display__campaign_v2.sql

with

sd_campaign_v2 as (

    select * from {{ source('sponsored_display', 'campaign_v2') }}

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
        currency,
        impressions,
        clicks,
        cost,
        campaign_budget,
        vtr,
        cumulative_reach,
        avg_impressions_frequency,
        attributed_units_ordered_1d,
        attributed_units_ordered_7d,
        attributed_units_ordered_14d,
        attributed_units_ordered_30d,
        attributed_conversions_1d,
        attributed_conversions_7d,
        attributed_conversions_14d,
        attributed_conversions_30d,
        attributed_conversions_1d_same_sku,
        attributed_conversions_7d_same_sku,
        attributed_conversions_14d_same_sku,
        attributed_conversions_30d_same_sku,
        attributed_sales_1d,
        attributed_sales_7d,
        attributed_sales_14d,
        attributed_sales_30d,
        attributed_sales_1d_same_sku,
        attributed_sales_7d_same_sku,
        attributed_sales_14d_same_sku,
        attributed_sales_30d_same_sku,
        attributed_units_ordered_new_to_brand_14d,
        attributed_orders_new_to_brand_14d,
        attributed_sales_new_to_brand_14d,
        attributed_branded_searches_14d,
        attributed_detail_page_view_14d,
        view_impressions,
        view_attributed_conversions_14d,
        view_attributed_units_ordered_14d,
        view_attributed_sales_14d,
        view_attributed_branded_searches_14d,
        view_attributed_detail_page_view_14d,
        view_attributed_sales_new_to_brand_14d,
        view_attributed_orders_new_to_brand_14d,
        view_attributed_units_ordered_new_to_brand_14d,
        video_unmutes,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views

    from sd_campaign_v2

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
        currency,

        -- numerics
        impressions,
        clicks,
        cost,
        campaign_budget,
        vtr,
        cumulative_reach,
        avg_impressions_frequency,
        attributed_units_ordered_1d,
        attributed_units_ordered_7d,
        attributed_units_ordered_14d,
        attributed_units_ordered_30d,
        attributed_conversions_1d,
        attributed_conversions_7d,
        attributed_conversions_14d,
        attributed_conversions_30d,
        attributed_conversions_1d_same_sku,
        attributed_conversions_7d_same_sku,
        attributed_conversions_14d_same_sku,
        attributed_conversions_30d_same_sku,
        attributed_sales_1d,
        attributed_sales_7d,
        attributed_sales_14d,
        attributed_sales_30d,
        attributed_sales_1d_same_sku,
        attributed_sales_7d_same_sku,
        attributed_sales_14d_same_sku,
        attributed_sales_30d_same_sku,
        attributed_units_ordered_new_to_brand_14d,
        attributed_orders_new_to_brand_14d,
        attributed_sales_new_to_brand_14d,
        attributed_branded_searches_14d,
        attributed_detail_page_view_14d,
        view_impressions,
        view_attributed_conversions_14d,
        view_attributed_units_ordered_14d,
        view_attributed_sales_14d,
        view_attributed_branded_searches_14d,
        view_attributed_detail_page_view_14d,
        view_attributed_sales_new_to_brand_14d,
        view_attributed_orders_new_to_brand_14d,
        view_attributed_units_ordered_new_to_brand_14d,
        video_unmutes,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
