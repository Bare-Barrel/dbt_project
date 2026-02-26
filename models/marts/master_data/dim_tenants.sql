-- dim_tenants.sql

with

tenants as (

    select * from {{ ref('stg_public__tenants') }}

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['tenant_id']) }} as tenant_sk,
        tenant_id,
        company_name

    from tenants

)

select * from add_surrogate_key
