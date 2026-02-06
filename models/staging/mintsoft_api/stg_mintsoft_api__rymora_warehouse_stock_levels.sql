-- stg_mintsoft_api__rymora_warehouse_stock_levels.sql

with

rymora_warehouse_stock_levels as (

    select * from {{ source('mintsoft_api','warehouse_stock_levels') }}

),

rename_fields as (

    select
        warehouse_name,
        account_name,
        recorded_at,

        productid as product_id,
        warehouseid as warehouse_id,
        clientid as client_id,
        "SKU" as sku,
        level as stock_level,
        totalstocklevel as total_stock_level,
        "PreOrderable" as preorderable,
        "Bundle" as bundle,
        lowstocklevel as low_stock_level,
        lastupdated as last_updated,
        "Breakdown" as breakdown

    from rymora_warehouse_stock_levels

),

cast_data_types as (

    select
        -- strings
        account_name,
        warehouse_name,
        sku,
        breakdown,

        CAST(warehouse_id as string) as warehouse_id,
        CAST(client_id as string) as client_id,
        CAST(product_id as string) as product_id,

        -- numerics
        stock_level,
        total_stock_level,
        low_stock_level,

        -- boolean
        preorderable,
        bundle,

        -- datetime
        recorded_at,

        TIMESTAMP(last_updated) as last_updated

    from rename_fields

),

add_recorded_date_field as (

    select
        *,
        DATE(recorded_at) as recorded_date

    from cast_data_types

)

select * from add_recorded_date_field
