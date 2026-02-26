-- obt_detail_page_sales_and_traffic_child.sql

with

detail_page_sales_and_traffic_child as (

    select * from {{ ref('stg_business_reports__detail_page_sales_and_traffic_child') }}

)

select * from detail_page_sales_and_traffic_child
