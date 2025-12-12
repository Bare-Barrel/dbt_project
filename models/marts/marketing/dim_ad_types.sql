-- dim_ad_types.sql

with

campaigns as (

    select * from {{ ref('int_union_campaigns') }}

),

select_fields as (

    select
        ad_type,
        tenant_id

    from campaigns

),

get_unique_placement_values as (

    select distinct *
    from select_fields

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['ad_type', 'tenant_id']) }} as ad_type_sk,
        ad_type,
        tenant_id

    from get_unique_placement_values

)

select * from add_surrogate_key
