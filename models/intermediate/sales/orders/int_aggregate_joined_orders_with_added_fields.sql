-- int_aggregate_joined_orders_with_added_fields.sql 06

{{ config(materialized='ephemeral') }}

with

agg_joined_orders_with_actual_amazon_fees as (

    select * from {{ ref('int_get_actual_amazon_fees_of_joined_orders') }}

),

aggregate_joined_orders_with_added_fields as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        item_price_currency_code,
        is_vine,
        is_replacement_order,
        tenant_id,
        actual_amazon_fee_currency_code,

        REGEXP_EXTRACT(seller_sku, r'[^_]+$') as product_code,
        SUM(quantity_ordered) as quantity_ordered,
        SUM(item_price_amount) as item_price_amount,
        SUM(promotion_discount_amount) as promotion_discount_amount,
        SUM(item_tax_amount) as item_tax_amount,
        SUM(shipping_price_amount) as shipping_price_amount,
        SUM(shipping_discount_amount) as shipping_discount_amount,
        SUM(buyer_info_gift_wrap_price_amount) as buyer_info_gift_wrap_price_amount,
        SUM(output_vat) as output_vat,
        SUM(coupon_fee) as coupon_fee,
        SUM(actual_amazon_fee_amount) as actual_amazon_fee_amount

    from agg_joined_orders_with_actual_amazon_fees

    group by
        purchase_date,
        marketplace,
        asin,
        seller_sku,
        item_price_currency_code,
        is_vine,
        is_replacement_order,
        tenant_id,
        order_status,
        actual_amazon_fee_currency_code

    order by purchase_date desc, marketplace desc, quantity_ordered desc
)

select * from aggregate_joined_orders_with_added_fields
