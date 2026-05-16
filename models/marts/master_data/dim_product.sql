-- dim_product.sql

with

all_products as (

    select * from {{ ref('int_union_all_products') }}

),

dim_portfolio_code as (

    select * from {{ ref('dim_portfolio_code') }}

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['asin']) }} as product_sk,
        *

    from all_products

),

add_portfolio_code_sk as (

    select
        ask.product_sk,

        dpc.portfolio_code_sk,

        ask.sku,
        ask.asin,
        ask.product_type,
        ask.tenant_id,
        ask.parent_code,
        ask.shaker_code,
        ask.portfolio_code,
        ask.product_code,
        ask.product_color_code,
        ask.product_color,
        ask.product_pack_size

    from add_surrogate_key as ask

    left join dim_portfolio_code as dpc
        on ask.portfolio_code = dpc.portfolio_code

)

select * from add_portfolio_code_sk
