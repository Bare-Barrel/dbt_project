-- obt_sqp_rankings.sql

with

search_query_performance_brand_view as (

    select * from {{ ref('stg_brand_analytics__search_query_performance_brand_view') }}

),

aggregate_search_query_performance_brand_view as (

    select
        marketplace,
        search_query,
        tenant_id,
        SUM(purchases_brand_count) as total_purchases_brand_count,
        RANK() over (partition by marketplace order by SUM(purchases_brand_count) desc) as rank

    from search_query_performance_brand_view

    where purchases_brand_count > 0

    group by marketplace, search_query, tenant_id

    order by marketplace desc, total_purchases_brand_count desc

),

rename_fields as (

    select
        marketplace,
        search_query as top_search_query,
        tenant_id,
        total_purchases_brand_count,
        rank

    from aggregate_search_query_performance_brand_view

)

select * from rename_fields
