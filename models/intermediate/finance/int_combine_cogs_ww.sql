-- int_combine_cogs_ww.sql 02

{{ config(materialized='view') }}

with

pivoted_bb_cogs_ww as (

    select
        start_date,
        end_date,
        parent_product,
        product_code,
        sku,
        asin,
        marketplace,
        currency_code,
        cogs,
        fba_fee,
        storage_fee,
        returns_cost,
        tenant_id

    from {{ ref('int_pivot_bb_cogs_ww') }}

),

pivoted_rymora_cogs_ww as (

    select
        start_date,
        end_date,
        parent_product,
        product_code,
        sku,
        asin,
        marketplace,
        currency_code,
        cogs,
        fba_fee,
        storage_fee,
        returns_cost,
        tenant_id

    from {{ ref('int_pivot_rymora_cogs_ww') }}

),

union_all_cogs_ww as (

    select * from pivoted_bb_cogs_ww

    union all

    select * from pivoted_rymora_cogs_ww

)

select * from union_all_cogs_ww
