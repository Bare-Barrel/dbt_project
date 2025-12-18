-- dim_placements.sql

with

unioned_campaign_placements as (

    select * from {{ ref('int_union_campaign_placements') }}

),

get_unique_placement_values as (

    select distinct placement_classification
    from unioned_campaign_placements

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['placement_classification']) }} as placement_sk,
        placement_classification

    from get_unique_placement_values

)

select * from add_surrogate_key
