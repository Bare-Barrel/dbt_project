-- obt_detail_page_sales_and_traffic_parent.sql TODO: remove unnecessary fields

with

detail_page_sales_and_traffic_parent as (

    select * from {{ ref('stg_business_reports__detail_page_sales_and_traffic_parent') }}

)

select * from detail_page_sales_and_traffic_parent
