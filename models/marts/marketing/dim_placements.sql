-- dim_placements.sql

with

unioned_campaign_placements as (

    select * from {{ ref('int_union_campaign_placements') }}

),

select_fields as (

    select
        placement_classification,
        tenant_id

    from unioned_campaign_placements

),

get_unique_placement_values as (

    select distinct *
    from select_fields

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['placement_classification', 'tenant_id']) }} as placement_sk,
        placement_classification,
        tenant_id

    from get_unique_placement_values

)

select * from add_surrogate_key
