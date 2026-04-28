-- obt_search_query_performance_weekly_asin.sql

with

search_query_performance_weekly_asin as (

    select * from {{ source('brand_analytics','search_query_performance_weekly_asin') }}

),

reorder_fields as (

    select
        start_date,
        end_date,
        created_at,
        updated_at,
        search_query,
        asin,
        marketplace,
        tenant_id,
        asin_median_click_price_currency_code,
        total_median_click_price_currency_code,
        asin_median_cart_add_price_currency_code,
        asin_median_purchase_price_currency_code,
        total_median_cart_add_price_currency_code,
        total_median_purchase_price_currency_code,
        asin_click_count,
        asin_click_share,
        asin_cart_add_count,
        asin_cart_add_share,
        asin_purchase_count,
        asin_purchase_share,
        asin_impression_count,
        asin_impression_share,
        asin_median_click_price,
        asin_median_click_price_amount,
        asin_median_cart_add_price,
        asin_median_cart_add_price_amount,
        asin_median_purchase_price,
        asin_median_purchase_price_amount,
        total_click_rate,
        total_click_count,
        total_cart_add_rate,
        total_cart_add_count,
        total_purchase_rate,
        total_purchase_count,
        total_median_cart_add_price,
        total_median_cart_add_price_amount,
        total_median_purchase_price,
        total_median_purchase_price_amount,
        total_median_click_price_amount,
        total_query_impression_count,
        total_one_day_shipping_click_count,
        total_one_day_shipping_cart_add_count,
        total_one_day_shipping_purchase_count,
        total_two_day_shipping_click_count,
        total_two_day_shipping_cart_add_count,
        total_two_day_shipping_purchase_count,
        total_same_day_shipping_click_count,
        total_same_day_shipping_cart_add_count,
        total_same_day_shipping_purchase_count,
        search_query_score,
        search_query_volume

    from search_query_performance_weekly_asin

)

select * from reorder_fields
