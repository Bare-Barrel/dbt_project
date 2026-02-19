-- obt_mintsoft_warehouse_stock_levels.sql

with

rymora_product_md_with_matched_tsp_skus as (

    select * from {{ ref('int_match_tsp_old_skus_to_current') }}

),

reorder_fields as (

    select
        client_id,
        account_name,
        warehouse_id,
        warehouse_name,
        product_id,
        sku,
        stock_level,
        total_stock_level,
        preorderable,
        bundle,
        low_stock_level,
        last_updated,
        breakdown,
        recorded_at,
        recorded_date

    from rymora_product_md_with_matched_tsp_skus

)

select * from reorder_fields
