-- int_convert_sd_advertised_product_amounts_to_usd.sql

{{ config(materialized='view') }}

with

sd_advertised_product as (

    select * from {{ ref('stg_sponsored_display__advertised_product') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

convert_amounts_to_usd as ( -- Retain all 14d amounts

    select
        sd_ap.campaign_date,
        sd_ap.created_at,
        sd_ap.updated_at,
        sd_ap.ad_id,
        sd_ap.ad_group_id,
        sd_ap.ad_group_name,
        sd_ap.campaign_id,
        sd_ap.campaign_name,
        sd_ap.promoted_sku,
        sd_ap.promoted_asin,
        sd_ap.bid_optimization,
        sd_ap.marketplace,
        sd_ap.tenant_id,
        sd_ap.impressions,
        sd_ap.impressions_views,
        sd_ap.impressions_frequency_average,
        sd_ap.clicks,
        sd_ap.units_sold,
        sd_ap.units_sold_clicks,
        sd_ap.new_to_brand_units_sold,
        sd_ap.new_to_brand_units_sold_clicks,
        sd_ap.purchases,
        sd_ap.purchases_clicks,
        sd_ap.purchases_promoted_clicks,
        sd_ap.new_to_brand_purchases,
        sd_ap.new_to_brand_purchases_clicks,
        sd_ap.add_to_cart,
        sd_ap.add_to_cart_clicks,
        sd_ap.add_to_cart_views,
        sd_ap.detail_page_views,
        sd_ap.detail_page_views_clicks,
        sd_ap.new_to_brand_detail_page_views,
        sd_ap.new_to_brand_ecpd_etail_page_view,
        sd_ap.new_to_brand_detail_page_view_rate,
        sd_ap.new_to_brand_detail_page_view_views,
        sd_ap.new_to_brand_detail_page_view_clicks,
        sd_ap.video_unmutes,
        sd_ap.video_complete_views,
        sd_ap.video_midpoint_views,
        sd_ap.video_first_quartile_views,
        sd_ap.video_third_quartile_views,
        sd_ap.add_to_cart_rate,
        sd_ap.e_cpa_dd_to_cart,
        sd_ap.branded_searches,
        sd_ap.branded_searches_clicks,
        sd_ap.branded_searches_views,
        sd_ap.branded_search_rate,
        sd_ap.e_cpb_rand_search,
        sd_ap.cumulative_reach,
        sd_ap.viewability_rate,
        sd_ap.view_click_through_rate,

        -- Amounts
        {# sd_ap.campaign_budget_currency_code,
        sd_ap.cost,
        sd_ap.sales,
        sd_ap.sales_clicks,
        sd_ap.sales_promoted_clicks,
        sd_ap.new_to_brand_sales,
        sd_ap.new_to_brand_sales_clicks, #}

        -- cost_usd
        case
            when sd_ap.campaign_budget_currency_code = "USD"
                then sd_ap.cost
            else SAFE_DIVIDE(sd_ap.cost, fx.rate)
        end as cost_usd,

        -- sales_usd
        case
            when sd_ap.campaign_budget_currency_code = "USD"
                then sd_ap.sales
            else SAFE_DIVIDE(sd_ap.sales, fx.rate)
        end as sales_usd,

        -- sales_clicks_usd
        case
            when sd_ap.campaign_budget_currency_code = "USD"
                then sd_ap.sales_clicks
            else SAFE_DIVIDE(sd_ap.sales_clicks, fx.rate)
        end as sales_clicks_usd,

        -- sales_promoted_clicks_usd
        case
            when sd_ap.campaign_budget_currency_code = "USD"
                then sd_ap.sales_promoted_clicks
            else SAFE_DIVIDE(sd_ap.sales_promoted_clicks, fx.rate)
        end as sales_promoted_clicks_usd,

        -- new_to_brand_sales_usd
        case
            when sd_ap.campaign_budget_currency_code = "USD"
                then sd_ap.new_to_brand_sales
            else SAFE_DIVIDE(sd_ap.new_to_brand_sales, fx.rate)
        end as new_to_brand_sales_usd,

        -- new_to_brand_sales_clicks_usd
        case
            when sd_ap.campaign_budget_currency_code = "USD"
                then sd_ap.new_to_brand_sales_clicks
            else SAFE_DIVIDE(sd_ap.new_to_brand_sales_clicks, fx.rate)
        end as new_to_brand_sales_clicks_usd

    from sd_advertised_product as sd_ap

    left join exchange_rates as fx
        on
            sd_ap.campaign_budget_currency_code = fx.target
            and DATE(sd_ap.campaign_date) = fx.recorded_at

)

select * from convert_amounts_to_usd
