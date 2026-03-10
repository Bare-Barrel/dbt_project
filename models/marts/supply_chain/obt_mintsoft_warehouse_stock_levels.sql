-- obt_mintsoft_warehouse_stock_levels.sql

with

rymora_mintsoft_inventory as (

    select * from {{ ref('int_get_allegro_inbound_units') }}

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
        on_order,
        preorderable,
        bundle,
        low_stock_level,
        last_updated,
        breakdown,
        recorded_at,
        recorded_date

    from rymora_mintsoft_inventory

)

select * from reorder_fields
