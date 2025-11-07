-- int_join_orders_and_order_items.sql

{{ config(materialized='ephemeral') }}

with

orders as (

    select * from {{ source('orders', 'amazon_orders') }}

),

order_items as (

    select * from {{ ref('stg_orders__amazon_order_items') }}

),

join_orders_and_order_items as (
    select
        o.amazon_order_id,
        oi.order_item_id,
        o.marketplace,
        o.sales_channel,
        oi.asin,
        oi.seller_sku,
        o.order_status,
        oi.quantity_ordered,
        oi.promotion_ids,
        oi.product_info_number_of_items,
        o.is_replacement_order,
        oi.item_price_amount,
        oi.item_price_currency_code,
        oi.item_tax_amount,
        oi.item_tax_currency_code,
        oi.promotion_discount_tax_amount,
        oi.promotion_discount_tax_currency_code,
        oi.promotion_discount_amount,
        oi.promotion_discount_currency_code,
        oi.tax_collection_model,
        oi.tax_collection_responsible_party,
        o.is_prime,
        o.replaced_order_id,
        oi.is_gift,
        o.tenant_id,
        o.purchase_date,
        oi.shipping_price_amount,
        oi.shipping_price_currency_code,
        oi.shipping_discount_amount,
        oi.shipping_discount_currency_code,
        oi.buyer_info_gift_wrap_price_amount,
        oi.buyer_info_gift_wrap_price_currency_code

    from orders as o

    left join order_items as oi
        on o.amazon_order_id = oi.amazon_order_id

    order by o.purchase_date desc, o.tenant_id asc, o.marketplace desc, oi.seller_sku asc

)

select * from join_orders_and_order_items
