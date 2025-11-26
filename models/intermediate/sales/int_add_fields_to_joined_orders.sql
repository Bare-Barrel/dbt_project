-- int_add_fields_to_joined_orders.sql 04
-- same as amazon_order_items_detailed_view

{{ config(materialized='view') }}

with

fields_to_add as (

    select * from {{ ref('int_calculate_fields_for_joined_orders') }}

),

with_prime_exclusive_price as (

    select * from {{ ref('int_get_prime_exclusive_price_for_new_orders') }}

),

joined_orders as (

    select * from {{ ref('int_join_orders_and_order_items') }}

),

add_fields_to_joined_orders as (

    select
        jo.amazon_order_id,
        jo.order_item_id,
        fta.purchase_datetime,
        fta.purchase_date,
        jo.marketplace,
        jo.sales_channel,
        jo.asin,
        jo.seller_sku,
        jo.order_status,
        jo.quantity_ordered,
        jo.promotion_ids,
        jo.product_info_number_of_items,
        jo.item_price_currency_code,
        prime.item_price_amount,
        jo.item_tax_amount,
        jo.promotion_discount_tax_currency_code,
        fta.output_vat,
        jo.item_tax_currency_code,
        jo.promotion_discount_tax_amount,
        jo.promotion_discount_currency_code,
        jo.promotion_discount_amount,
        fta.coupon_fee,
        jo.tax_collection_model,
        jo.tax_collection_responsible_party,
        jo.is_prime,
        jo.is_replacement_order,
        jo.replaced_order_id,
        jo.is_gift,
        fta.is_vine,
        jo.tenant_id,
        jo.shipping_price_amount,
        jo.shipping_price_currency_code,
        jo.shipping_discount_amount,
        jo.shipping_discount_currency_code,
        jo.buyer_info_gift_wrap_price_amount,
        jo.buyer_info_gift_wrap_price_currency_code

    from joined_orders as jo

    left join fields_to_add as fta
        on jo.order_item_id = fta.order_item_id

    left join with_prime_exclusive_price as prime
        on jo.order_item_id = prime.order_item_id

    order by fta.purchase_datetime desc, jo.tenant_id asc, jo.marketplace desc, jo.seller_sku asc
)

select * from add_fields_to_joined_orders
