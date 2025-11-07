-- stg_orders__amazon_orders.sql

with

source as (

    select * from `modern-sublime-383117`.`orders`.`amazon_orders`

),

handled_nulls as (

    select
        -- ids
        tenant_id,
        marketplace_id,
        amazon_order_id,
        seller_order_id,
        replaced_order_id,

        -- strings
        order_type,
        marketplace,
        order_status,
        sales_channel,
        payment_method,
        ship_service_level,
        fulfillment_channel,
        shipping_address_city,
        buyer_info_buyer_email,
        payment_method_details,
        order_total_currency_code,
        shipping_address_postal_code,
        shipping_address_country_code,
        shipment_service_level_category,
        shipping_address_state_or_region,

        -- numerics
        order_total_amount,
        number_of_items_shipped,
        number_of_items_unshipped,

        -- booleans
        is_ispu,
        is_prime,
        is_sold_by_ab,
        is_premium_order,
        is_business_order,
        has_regulated_items,
        is_replacement_order,
        is_access_point_order,
        is_global_express_enabled,

        -- dates and timestamps
        created_at,
        updated_at,
        purchase_date,
        last_update_date,
        latest_ship_date,
        earliest_ship_date

    from source

)

select * from handled_nulls