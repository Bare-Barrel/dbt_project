-- obt_order_locations.sql

with

orders as (

    select * from {{ ref('stg_orders__amazon_orders') }}

),

select_location_fields as (

    select
        amazon_order_id,
        purchase_date,
        order_status,
        marketplace,
        tenant_id,
        shipping_address_city,
        shipping_address_state_or_region,
        shipping_address_country_code,
        shipping_address_postal_code,
        shipment_service_level_category

    from orders

    order by purchase_date desc

)

select * from select_location_fields
