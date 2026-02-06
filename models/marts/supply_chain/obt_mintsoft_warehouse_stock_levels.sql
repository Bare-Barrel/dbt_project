-- obt_mintsoft_warehouse_stock_levels.sql

with

rymora_warehouse_stock_levels as (

    select * from {{ ref('stg_mintsoft_api__rymora_warehouse_stock_levels') }}

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

    from rymora_warehouse_stock_levels

)

select * from reorder_fields
