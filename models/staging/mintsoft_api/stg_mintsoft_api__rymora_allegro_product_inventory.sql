-- stg_mintsoft_api__rymora_allegro_product_inventory.sql

with

rymora_allegro_product_inventory as (

    select * from {{ source('mintsoft_api','allegro_product_inventory') }}

),

rename_fields as (

    select
        account_name,
        recorded_at,

        productid as product_id,
        stocklevel as stock_level,
        `Allocated` as allocated,
        onhand as on_hand,
        offhand as off_hand,
        awaitingreplen as awaiting_replen,
        onorder as on_order,
        requiredbybackorder as required_by_back_order,
        inquarantine as in_quarantine,
        intransit as in_transit,
        intransition as in_transition,
        `Scrapped` as scrapped,
        `SKU` as sku,
        warehouseid as warehouse_id,
        locationid as location_id,
        `Breakdown` as breakdown,
        `ID` as id,
        lastupdated as last_updated,
        lastupdatedbyuser as last_updated_by_user

    from rymora_allegro_product_inventory

),

cast_data_types as (

    select
        -- ids
        CAST(product_id as string) as product_id,
        CAST(warehouse_id as string) as warehouse_id,
        CAST(location_id as string) as location_id,
        CAST(id as string) as id,

        -- strings
        account_name,
        sku,
        breakdown,
        last_updated_by_user,

        -- numerics
        stock_level,
        allocated,
        on_hand,
        off_hand,
        awaiting_replen,
        on_order,
        required_by_back_order,
        in_quarantine,
        in_transit,
        in_transition,
        scrapped,

        -- datetime
        recorded_at,
        PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*S', last_updated) as last_updated

    from rename_fields

),

add_recorded_date_field as (

    select
        *,
        DATE(recorded_at) as recorded_date

    from cast_data_types

)

select * from add_recorded_date_field
