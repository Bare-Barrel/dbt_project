-- obt_search_query_performance_brand_view.sql

with

search_query_performance_brand_view as (

    select * from {{ ref('stg_brand_analytics__search_query_performance_brand_view') }}

)

select * from search_query_performance_brand_view
