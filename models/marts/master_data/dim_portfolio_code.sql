-- dim_portfolio_code.sql

with

dim_product as (

    select * from {{ ref('dim_product') }}

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

)

select * from add_surrogate_key
