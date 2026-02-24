-- stg_sponsored_brands__campaign_console.sql

with

sb_campaign_console as (

    select * from {{ source('sponsored_brands', 'campaign_console') }}

),

rename_fields as (

    select
        date as campaign_date,
        created_at,
        updated_at,
        campaign_name,
        portfolio_name,
        marketplace,
        tenant_id,
        currency,
        impressions,
        clicks,
        spend,
        _14_day_total_units,
        _14_day_total_orders,
        _14_day_total_sales,
        cost_per_click,
        click_thru_rate,
        click_through_rate_for_views,
        _14_day_conversion_rate,
        _14_day_branded_searches,
        _14_day_detail_page_views,
        _14_day_new_to_brand_sales,
        _14_day_new_to_brand_units,
        _14_day_new_to_brand_orders,
        _14_day_of_sales_new_to_brand,
        _14_day_of_units_new_to_brand,
        _14_day_of_orders_new_to_brand,
        _14_day_new_to_brand_order_rate,
        total_advertising_cost_of_sales,
        total_return_on_advertising_spend,
        view_through_rate,
        video_unmutes,
        video_complete_views,
        video_midpoint_views,
        video_first_quartile_views,
        video_third_quartile_views,
        _5_second_views,
        _5_second_view_rate,
        viewable_impressions

    from sb_campaign_console

)

select * from rename_fields
