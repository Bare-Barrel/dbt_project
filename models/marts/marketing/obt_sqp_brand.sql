-- obt_sqp_brand.sql

with

sqp_brand as (

    select * from {{ source('brand_analytics','search_query_performance_brand_view') }}

),

remove_airbyte_fields as (

    select
        week,
        start_date,
        end_date,
        created_at,
        updated_at,
        reporting_date,
        reporting_range,
        tenant_id,
        marketplace,
        search_query,
        clicks_price,
        cart_adds_price,
        purchases_price,
        clicks_click_rate,
        clicks_brand_count,
        clicks_brand_price,
        clicks_brand_share,
        clicks_total_count,
        search_query_score,
        search_query_volume,
        cart_adds_brand_count,
        cart_adds_brand_price,
        cart_adds_brand_share,
        cart_adds_total_count,
        purchases_brand_count,
        purchases_brand_price,
        purchases_brand_share,
        purchases_total_count,
        cart_adds_cart_add_rate,
        impressions_brand_count,
        impressions_brand_share,
        impressions_total_count,
        purchases_purchase_rate,
        clicks_1d_shipping_speed,
        clicks_2d_shipping_speed,
        cart_adds_1d_shipping_speed,
        cart_adds_2d_shipping_speed,
        purchases_1d_shipping_speed,
        purchases_2d_shipping_speed,
        clicks_same_day_shipping_speed,
        cart_adds_same_day_shipping_speed,
        purchases_same_day_shipping_speed

    from sqp_brand

)

select * from remove_airbyte_fields
