-- int_get_sb_placement_asin.sql sb_cp_05

{{ config(materialized='view') }}

with

sb_placements_with_product_codes as (

    select * from {{ ref('int_get_sb_placement_product_codes') }}

),

dim_products as (

    select * from {{ ref('dim_products') }}

),

get_asin_for_bb as (

    select
        sb_cp_w_pc.*,
        prod.asin

    from sb_placements_with_product_codes as sb_cp_w_pc

    left join dim_products as prod
        on
            sb_cp_w_pc.parent_code = prod.parent_code
            and sb_cp_w_pc.portfolio_code = prod.portfolio_code
            and sb_cp_w_pc.product_code = prod.product_code

    where sb_cp_w_pc.tenant_id = 1

),

get_asin_for_ry as (

    select
        sb_cp_w_pc.*,
        prod.asin

    from sb_placements_with_product_codes as sb_cp_w_pc

    left join dim_products as prod
        on
            sb_cp_w_pc.parent_code = prod.parent_code
            and sb_cp_w_pc.product_color = prod.product_color
            and sb_cp_w_pc.product_pack_size = prod.product_pack_size

    where sb_cp_w_pc.tenant_id = 2

),

union_bb_and_ry as (

    select * from get_asin_for_bb

    union all

    select * from get_asin_for_ry

)

select * from union_bb_and_ry
