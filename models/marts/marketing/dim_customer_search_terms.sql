-- dim_customer_search_terms.sql

with

stg_search_term as (

    select * from {{ ref('stg_sponsored_products__search_term') }}

),

get_distinct_search_terms as (

    select distinct LOWER(search_term) as search_term

    from stg_search_term

)

select * from get_distinct_search_terms
