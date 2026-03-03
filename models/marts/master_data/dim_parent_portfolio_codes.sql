-- dim_parent_portfolio_codes.sql

with

dim_products as (

    select * from {{ ref('dim_products') }}

),

select_fields as (

    select
        portfolio_code,
        parent_code,
        product_type,
        tenant_id

    from dim_products

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

)

select * from remove_duplicates
