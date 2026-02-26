-- stg_brand_analytics__search_query_performance_brand_view.sql

with

search_query_performance_asin_view as (

    select * from {{ source('brand_analytics','search_query_performance_brand_view') }}

),

rename_fields as (

    select
        reporting_date,
        start_date,
        end_date,
        created_at,
        updated_at,
        week as week_num,
        reporting_range,
        search_query,
        marketplace,
        tenant_id,
        clicks_price,
        clicks_click_rate,
        clicks_brand_count,
        clicks_brand_price,
        clicks_brand_share,
        clicks_total_count,
        clicks_1d_shipping_speed,
        clicks_2d_shipping_speed,
        clicks_same_day_shipping_speed,
        cart_adds_price,
        cart_adds_brand_count,
        cart_adds_brand_price,
        cart_adds_brand_share,
        cart_adds_total_count,
        cart_adds_cart_add_rate,
        cart_adds_1d_shipping_speed,
        cart_adds_2d_shipping_speed,
        cart_adds_same_day_shipping_speed,
        purchases_price,
        purchases_brand_count,
        purchases_brand_price,
        purchases_brand_share,
        purchases_total_count,
        purchases_purchase_rate,
        purchases_1d_shipping_speed,
        purchases_2d_shipping_speed,
        purchases_same_day_shipping_speed,
        search_query_score,
        search_query_volume,
        impressions_brand_count,
        impressions_brand_share,
        impressions_total_count

    from search_query_performance_asin_view

),

cast_data_types as (

    select
        -- ids
        tenant_id,

        -- strings
        reporting_range,
        search_query,
        marketplace,

        -- numerics
        CAST(week_num as integer) as week_num,
        clicks_price,
        clicks_click_rate,
        clicks_brand_count,
        clicks_brand_price,
        clicks_brand_share,
        clicks_total_count,
        clicks_1d_shipping_speed,
        clicks_2d_shipping_speed,
        clicks_same_day_shipping_speed,
        cart_adds_price,
        cart_adds_brand_count,
        cart_adds_brand_price,
        cart_adds_brand_share,
        cart_adds_total_count,
        cart_adds_cart_add_rate,
        cart_adds_1d_shipping_speed,
        cart_adds_2d_shipping_speed,
        cart_adds_same_day_shipping_speed,
        purchases_price,
        purchases_brand_count,
        purchases_brand_price,
        purchases_brand_share,
        purchases_total_count,
        purchases_purchase_rate,
        purchases_1d_shipping_speed,
        purchases_2d_shipping_speed,
        purchases_same_day_shipping_speed,
        search_query_score,
        search_query_volume,
        impressions_brand_count,
        impressions_brand_share,
        impressions_total_count,

        -- datetime
        reporting_date,
        start_date,
        end_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
