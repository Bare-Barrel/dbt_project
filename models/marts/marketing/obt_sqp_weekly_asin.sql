-- obt_sqp_weekly_asin.sql

with

sqp_weekly_asin_usd as (

    select * from {{ ref('int_convert_sqp_weekly_asin_amounts_to_usd') }}

),

reorder_fields as (

    select
        asin,
        start_date,
        end_date,
        created_at,
        updated_at,
        tenant_id,
        marketplace,
        search_query,
        search_query_score,
        search_query_volume,
        asin_click_count,
        asin_click_share,
        asin_cart_add_count,
        asin_cart_add_share,
        asin_purchase_count,
        asin_purchase_share,
        asin_impression_count,
        asin_impression_share,
        asin_median_click_price,
        asin_median_click_price_amount_usd,
        asin_median_cart_add_price,
        asin_median_cart_add_price_amount_usd,
        asin_median_purchase_price,
        asin_median_purchase_price_amount_usd,
        total_click_count,
        total_click_rate,
        total_cart_add_count,
        total_cart_add_rate,
        total_purchase_count,
        total_purchase_rate,
        total_median_cart_add_price,
        total_median_cart_add_price_amount_usd,
        total_median_purchase_price,
        total_median_purchase_price_amount_usd,
        total_median_click_price_amount_usd,
        total_query_impression_count,
        total_one_day_shipping_click_count,
        total_one_day_shipping_cart_add_count,
        total_one_day_shipping_purchase_count,
        total_two_day_shipping_click_count,
        total_two_day_shipping_cart_add_count,
        total_two_day_shipping_purchase_count,
        total_same_day_shipping_click_count,
        total_same_day_shipping_cart_add_count,
        total_same_day_shipping_purchase_count

    from sqp_weekly_asin_usd

)

select * from reorder_fields
