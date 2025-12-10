-- dim_tenants.sql

with

tenants as (

    select * from {{ source('public', 'tenants') }}

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['tenant_id']) }} as tenant_sk,
        tenant_id,
        company

    from tenants

)

select * from add_surrogate_key
