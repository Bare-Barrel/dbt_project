-- dim_portfolio_code.sql

with

dim_product as (

    select * from {{ ref('dim_product') }}

),

dim_tenant as (

    select * from {{ ref('dim_tenant') }}

),

select_fields as (

    select
        portfolio_code,
        parent_code,
        product_type,
        tenant_id

    from dim_product

),

get_unique_rows as (

    select distinct * from select_fields

),

remove_duplicates as (

    select
        portfolio_code,
        parent_code,
        product_type,
        tenant_id

    from (
        select
            portfolio_code,
            parent_code,
            product_type,
            tenant_id,
            row_number() over (
                partition by portfolio_code
                order by product_type asc
            ) as row_num
        from get_unique_rows
    ) as t

    where row_num = 1

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['portfolio_code']) }} as portfolio_code_sk,
        *

    from remove_duplicates

),

add_tenant_sk as (

    select
        dtn.tenant_sk,
        ask.portfolio_code_sk,
        ask.portfolio_code,
        ask.parent_code,
        ask.product_type

    from add_surrogate_key as ask

    left join dim_tenant as dtn
        on ask.tenant_id = dtn.tenant_id

)

select * from add_tenant_sk
