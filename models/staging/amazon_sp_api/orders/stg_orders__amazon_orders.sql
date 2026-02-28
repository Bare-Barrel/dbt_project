-- stg_orders__amazon_orders.sql

with

orders as (

    select * from {{ source('orders','amazon_orders') }}

),

cast_data_types as (

    select
        -- ids
        amazon_order_id,
        seller_order_id,
        marketplace_id,
        replaced_order_id,
        tenant_id,

        -- strings
        order_type,
        marketplace,
        order_status,
        sales_channel,
        payment_method,
        payment_method_details,
        ship_service_level,
        fulfillment_channel,
        buyer_info_buyer_email,
        shipping_address_city,
        shipping_address_state_or_region,
        shipping_address_country_code,
        shipping_address_postal_code,
        shipment_service_level_category,
        order_total_currency_code,

        -- numerics
        number_of_items_shipped,
        number_of_items_unshipped,
        order_total_amount,

        -- boolean
        is_ispu,
        is_prime,
        is_sold_by_ab,
        is_premium_order,
        is_business_order,
        has_regulated_items,
        is_replacement_order,
        is_access_point_order,
        is_global_express_enabled,

        -- datetime
        purchase_date as purchase_datetime,
        DATE(purchase_date) as purchase_date,
        last_update_date,
        latest_ship_date,
        earliest_ship_date,
        created_at,
        updated_at

    from orders

)

select * from cast_data_types
