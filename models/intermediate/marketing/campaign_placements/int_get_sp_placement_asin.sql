-- int_get_sp_placement_asin.sql sp_cp_05

{{ config(materialized='view') }}

with

sp_placements_with_product_codes as (

    select * from {{ ref('int_get_sp_placement_product_codes') }}

),

dim_products as (

    select * from {{ ref('dim_products') }}

),

get_asin_for_bb as (

    select
        sp_cp_w_pc.*,
        prod.asin

    from sp_placements_with_product_codes as sp_cp_w_pc

    left join dim_products as prod
        on
            sp_cp_w_pc.parent_code = prod.parent_code
            and sp_cp_w_pc.portfolio_code = prod.portfolio_code
            and sp_cp_w_pc.product_code = prod.product_code

    where sp_cp_w_pc.tenant_id = 1

),

get_asin_for_ry as (

    select
        sp_cp_w_pc.*,
        prod.asin

    from sp_placements_with_product_codes as sp_cp_w_pc

    left join dim_products as prod
        on
            sp_cp_w_pc.parent_code = prod.parent_code
            and sp_cp_w_pc.product_color = prod.product_color
            and sp_cp_w_pc.product_pack_size = prod.product_pack_size

    where sp_cp_w_pc.tenant_id = 2

),

union_bb_and_ry as (

    select * from get_asin_for_bb

    union all

    select * from get_asin_for_ry

)

select * from union_bb_and_ry
