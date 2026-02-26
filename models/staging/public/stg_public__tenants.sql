-- stg_public__tenants.sql

with

tenants as (

    select * from {{ source('public','tenants') }}

),

remove_airbyte_fields as (

    select
        created_at,
        updated_at,
        tenant_id,
        company as company_name

    from tenants

)

select * from remove_airbyte_fields
