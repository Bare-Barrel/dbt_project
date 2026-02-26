-- stg_business_reports__detail_page_sales_and_traffic_child.sql

with

detail_page_sales_and_traffic_child as (

    select * from {{ source('business_reports','detail_page_sales_and_traffic_child') }}

),

rename_fields as (

    select
        date as record_date,
        created_at,
        updated_at,
        parent_asin,
        child_asin,
        marketplace,
        tenant_id,
        sales_by_asin_ordered_product_sales_currency_code,
        sales_by_asin_ordered_product_sales_b2b_currency_code,
        traffic_by_asin_sessions,
        traffic_by_asin_sessions_b2b,
        traffic_by_asin_session_percentage,
        traffic_by_asin_session_percentage_b2b,
        traffic_by_asin_page_views,
        traffic_by_asin_page_views_b2b,
        traffic_by_asin_page_views_percentage,
        traffic_by_asin_page_views_percentage_b2b,
        traffic_by_asin_browser_sessions,
        traffic_by_asin_browser_sessions_b2b,
        traffic_by_asin_browser_session_percentage,
        traffic_by_asin_browser_session_percentage_b2b,
        traffic_by_asin_browser_page_views,
        traffic_by_asin_browser_page_views_b2b,
        traffic_by_asin_browser_page_views_percentage,
        traffic_by_asin_browser_page_views_percentage_b2b,
        traffic_by_asin_buy_box_percentage,
        traffic_by_asin_buy_box_percentage_b2b,
        traffic_by_asin_mobile_app_sessions,
        traffic_by_asin_mobile_app_sessions_b2b,
        traffic_by_asin_mobile_app_session_percentage,
        traffic_by_asin_mobile_app_session_percentage_b2b,
        traffic_by_asin_mobile_app_page_views,
        traffic_by_asin_mobile_app_page_views_b2b,
        traffic_by_asin_mobile_app_page_views_percentage,
        traffic_by_asin_mobile_app_page_views_percentage_b2b,
        traffic_by_asin_unit_session_percentage,
        traffic_by_asin_unit_session_percentage_b2b,
        sales_by_asin_units_ordered,
        sales_by_asin_units_ordered_b2b,
        sales_by_asin_total_order_items,
        sales_by_asin_total_order_items_b2b,
        sales_by_asin_ordered_product_sales_amount,
        sales_by_asin_ordered_product_sales_b2b_amount

    from detail_page_sales_and_traffic_child

)

select * from rename_fields
