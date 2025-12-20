-- int_convert_sqp_weekly_asin_amounts_to_usd.sql

{{ config(materialized='view') }}

with

sqp_weekly_asin as (

    select * from {{ source('brand_analytics','search_query_performance_weekly_asin') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_campaign_placement_and_fx_rates as (

    select
        sqp.asin,
        sqp.start_date,
        sqp.end_date,
        sqp.created_at,
        sqp.updated_at,
        sqp.tenant_id,
        sqp.marketplace,
        sqp.search_query,
        sqp.search_query_score,
        sqp.search_query_volume,
        sqp.asin_click_count,
        sqp.asin_click_share,
        sqp.asin_cart_add_count,
        sqp.asin_cart_add_share,
        sqp.asin_purchase_count,
        sqp.asin_purchase_share,
        sqp.asin_impression_count,
        sqp.asin_impression_share,
        sqp.asin_median_click_price,
        sqp.asin_median_click_price_amount,
        sqp.asin_median_cart_add_price,
        sqp.asin_median_cart_add_price_amount,
        sqp.asin_median_purchase_price,
        sqp.asin_median_purchase_price_amount,
        sqp.total_click_count,
        sqp.total_click_rate,
        sqp.total_cart_add_count,
        sqp.total_cart_add_rate,
        sqp.total_purchase_count,
        sqp.total_purchase_rate,
        sqp.total_median_cart_add_price,
        sqp.total_median_cart_add_price_amount,
        sqp.total_median_purchase_price,
        sqp.total_median_purchase_price_amount,
        sqp.total_median_click_price_amount,
        sqp.total_query_impression_count,
        sqp.total_one_day_shipping_click_count,
        sqp.total_one_day_shipping_cart_add_count,
        sqp.total_one_day_shipping_purchase_count,
        sqp.total_two_day_shipping_click_count,
        sqp.total_two_day_shipping_cart_add_count,
        sqp.total_two_day_shipping_purchase_count,
        sqp.total_same_day_shipping_click_count,
        sqp.total_same_day_shipping_cart_add_count,
        sqp.total_same_day_shipping_purchase_count,

        case
            when sqp.asin_median_click_price_currency_code = "USD"
                then 1
            else fx.rate
        end as fx_rate

    from sqp_weekly_asin as sqp

    left join exchange_rates as fx
        on
            sqp.asin_median_click_price_currency_code = fx.target
            and DATE(sqp.start_date) = fx.recorded_at

),

convert_amounts_to_usd as (

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
        asin_median_cart_add_price,
        asin_median_purchase_price,
        total_click_count,
        total_click_rate,
        total_cart_add_count,
        total_cart_add_rate,
        total_purchase_count,
        total_purchase_rate,
        total_median_cart_add_price,
        total_median_purchase_price,
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

        SAFE_DIVIDE(asin_median_click_price_amount, fx_rate) as asin_median_click_price_amount_usd,
        SAFE_DIVIDE(asin_median_cart_add_price_amount, fx_rate) as asin_median_cart_add_price_amount_usd,
        SAFE_DIVIDE(asin_median_purchase_price_amount, fx_rate) as asin_median_purchase_price_amount_usd,
        SAFE_DIVIDE(total_median_cart_add_price_amount, fx_rate) as total_median_cart_add_price_amount_usd,
        SAFE_DIVIDE(total_median_purchase_price_amount, fx_rate) as total_median_purchase_price_amount_usd,
        SAFE_DIVIDE(total_median_click_price_amount, fx_rate) as total_median_click_price_amount_usd

    from join_campaign_placement_and_fx_rates

)

select * from convert_amounts_to_usd
