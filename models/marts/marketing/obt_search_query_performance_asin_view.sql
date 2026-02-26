-- obt_search_query_performance_asin_view.sql

with

search_query_performance_asin_view as (

    select * from {{ ref('stg_brand_analytics__search_query_performance_asin_view') }}

)

select * from search_query_performance_asin_view
