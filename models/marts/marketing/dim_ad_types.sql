-- dim_ad_types.sql

with

campaigns as (

    select * from {{ ref('int_union_all_campaigns') }}

),

get_unique_placement_values as (

    select distinct ad_type
    from campaigns

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['ad_type']) }} as ad_type_sk,
        ad_type

    from get_unique_placement_values

)

select * from add_surrogate_key
