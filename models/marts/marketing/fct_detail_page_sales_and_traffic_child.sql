-- fct_detail_page_sales_and_traffic_child.sql

with

detail_page_sales_and_traffic_child as (

    select * from {{ ref('stg_business_reports__detail_page_sales_and_traffic_child') }}

),

dim_marketplace as (

    select * from {{ ref('dim_marketplace') }}

),

dim_product as (

    select * from {{ ref('dim_product') }}

),

dim_tenant as (

    select * from {{ ref('dim_tenant') }}

),

add_surrogate_keys as (

    select
        dpst_c.record_date,
        dpst_c.created_at,
        dpst_c.updated_at,
        dpst_c.parent_asin,
        dpst_c.sales_by_asin_ordered_product_sales_currency_code,
        dpst_c.sales_by_asin_ordered_product_sales_b2b_currency_code,

        dpr.product_sk,
        dmp.marketplace_sk,
        dtn.tenant_sk,

        dpst_c.traffic_by_asin_sessions,
        dpst_c.traffic_by_asin_sessions_b2b,
        dpst_c.traffic_by_asin_session_percentage,
        dpst_c.traffic_by_asin_session_percentage_b2b,
        dpst_c.traffic_by_asin_page_views,
        dpst_c.traffic_by_asin_page_views_b2b,
        dpst_c.traffic_by_asin_page_views_percentage,
        dpst_c.traffic_by_asin_page_views_percentage_b2b,
        dpst_c.traffic_by_asin_browser_sessions,
        dpst_c.traffic_by_asin_browser_sessions_b2b,
        dpst_c.traffic_by_asin_browser_session_percentage,
        dpst_c.traffic_by_asin_browser_session_percentage_b2b,
        dpst_c.traffic_by_asin_browser_page_views,
        dpst_c.traffic_by_asin_browser_page_views_b2b,
        dpst_c.traffic_by_asin_browser_page_views_percentage,
        dpst_c.traffic_by_asin_browser_page_views_percentage_b2b,
        dpst_c.traffic_by_asin_buy_box_percentage,
        dpst_c.traffic_by_asin_buy_box_percentage_b2b,
        dpst_c.traffic_by_asin_mobile_app_sessions,
        dpst_c.traffic_by_asin_mobile_app_sessions_b2b,
        dpst_c.traffic_by_asin_mobile_app_session_percentage,
        dpst_c.traffic_by_asin_mobile_app_session_percentage_b2b,
        dpst_c.traffic_by_asin_mobile_app_page_views,
        dpst_c.traffic_by_asin_mobile_app_page_views_b2b,
        dpst_c.traffic_by_asin_mobile_app_page_views_percentage,
        dpst_c.traffic_by_asin_mobile_app_page_views_percentage_b2b,
        dpst_c.traffic_by_asin_unit_session_percentage,
        dpst_c.traffic_by_asin_unit_session_percentage_b2b,
        dpst_c.sales_by_asin_units_ordered,
        dpst_c.sales_by_asin_units_ordered_b2b,
        dpst_c.sales_by_asin_total_order_items,
        dpst_c.sales_by_asin_total_order_items_b2b,
        dpst_c.sales_by_asin_ordered_product_sales_amount,
        dpst_c.sales_by_asin_ordered_product_sales_b2b_amount

    from detail_page_sales_and_traffic_child as dpst_c

    left join dim_product as dpr
        on dpst_c.child_asin = dpr.asin

    left join dim_marketplace as dmp
        on dpst_c.marketplace = dmp.marketplace_name

    left join dim_tenant as dtn
        on dpst_c.tenant_id = dtn.tenant_id

)

select * from add_surrogate_keys
