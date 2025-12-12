-- dim_placements.sql

with

unioned_campaign_placements as (

    select * from {{ ref('int_union_campaign_placements') }}

),

dim_tenants as (

    select * from {{ ref('dim_tenants') }}

),

select_fields as (

    select
        placement_classification,
        tenant_id

    from unioned_campaign_placements

),

get_unique_values as (

    select distinct *
    from select_fields

),

get_tenant_sk as (

    select
        guv.placement_classification,
        dt.tenant_sk

    from get_unique_values as guv

    left join dim_tenants as dt
        on guv.tenant_id = dt.tenant_id

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['placement_classification', 'tenant_sk']) }} as placement_sk,
        placement_classification,
        tenant_sk

    from get_tenant_sk

)

select * from add_surrogate_key
