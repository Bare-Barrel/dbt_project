-- dim_ad_types.sql

with

campaigns as (

    select * from {{ ref('int_union_campaigns') }}

),

dim_tenants as (

    select * from {{ ref('dim_tenants') }}

),

select_fields as (

    select
        ad_type,
        tenant_id

    from campaigns

),

get_unique_values as (

    select distinct *
    from select_fields

),

get_tenant_sk as (

    select
        guv.ad_type,
        dt.tenant_sk

    from get_unique_values as guv

    left join dim_tenants as dt
        on guv.tenant_id = dt.tenant_id

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['ad_type', 'tenant_sk']) }} as ad_type_sk,
        ad_type,
        tenant_sk

    from get_tenant_sk

)

select * from add_surrogate_key
