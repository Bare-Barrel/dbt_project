-- int_convert_datetime_to_marketplace_tz.sql 05

{{ config(materialized='view') }}

with

joined_orders_with_added_fields as (

    select * from {{ ref('int_add_fields_to_joined_orders') }}

),

convert_utc_to_local_marketplace_tz as (

    select
        amazon_order_id,
        order_item_id,
        marketplace,
        sales_channel,
        asin,
        seller_sku,
        order_status,
        quantity_ordered,
        promotion_ids,
        product_info_number_of_items,
        item_price_currency_code,
        item_price_amount,
        item_tax_amount,
        promotion_discount_tax_currency_code,
        uk_output_vat,
        item_tax_currency_code,
        promotion_discount_tax_amount,
        promotion_discount_currency_code,
        promotion_discount_amount,
        coupon_fee,
        tax_collection_model,
        tax_collection_responsible_party,
        is_prime,
        is_replacement_order,
        replaced_order_id,
        is_gift,
        is_vine,
        tenant_id,
        shipping_price_amount,
        shipping_price_currency_code,
        shipping_discount_amount,
        shipping_discount_currency_code,
        buyer_info_gift_wrap_price_amount,
        buyer_info_gift_wrap_price_currency_code,

        -- UTC
        purchase_datetime as purchase_datetime_utc,
        purchase_date as purchase_date_utc,

        -- Local marketplace timezone
        {{ marketplace_tz('purchase_datetime', 'marketplace') }} as purchase_datetime_local

    from joined_orders_with_added_fields

),

add_purchase_date_local as (

    select
        *,
        DATE(purchase_datetime_local) as purchase_date_local

    from convert_utc_to_local_marketplace_tz

)

select * from add_purchase_date_local
