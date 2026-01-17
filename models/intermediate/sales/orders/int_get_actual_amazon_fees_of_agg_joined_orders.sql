-- int_get_actual_amazon_fees_of_agg_joined_orders.sql 05

{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

with

joined_orders_with_added_fields as (

    select * from {{ ref('int_add_fields_to_joined_orders') }}

),

aggregated_financial_events_item_fees as (

    select * from {{ ref('int_aggregate_financial_events_item_fees') }}

),

get_actual_amazon_fees as (

    select
        jo_w_af.amazon_order_id,
        jo_w_af.order_item_id,
        jo_w_af.purchase_datetime,
        jo_w_af.purchase_date,
        jo_w_af.marketplace,
        jo_w_af.sales_channel,
        jo_w_af.asin,
        jo_w_af.seller_sku,
        jo_w_af.order_status,
        jo_w_af.quantity_ordered,
        jo_w_af.promotion_ids,
        jo_w_af.product_info_number_of_items,
        jo_w_af.item_price_currency_code,
        jo_w_af.item_price_amount,
        jo_w_af.item_tax_amount,
        jo_w_af.promotion_discount_tax_currency_code,
        jo_w_af.output_vat,
        jo_w_af.item_tax_currency_code,
        jo_w_af.promotion_discount_tax_amount,
        jo_w_af.promotion_discount_currency_code,
        jo_w_af.promotion_discount_amount,
        jo_w_af.coupon_fee,
        jo_w_af.tax_collection_model,
        jo_w_af.tax_collection_responsible_party,
        jo_w_af.is_prime,
        jo_w_af.is_replacement_order,
        jo_w_af.replaced_order_id,
        jo_w_af.is_gift,
        jo_w_af.is_vine,
        jo_w_af.tenant_id,
        jo_w_af.shipping_price_amount,
        jo_w_af.shipping_price_currency_code,
        jo_w_af.shipping_discount_amount,
        jo_w_af.shipping_discount_currency_code,
        jo_w_af.buyer_info_gift_wrap_price_amount,
        jo_w_af.buyer_info_gift_wrap_price_currency_code,
        agg_feif.item_fee__fee_amount as actual_amazon_fee_amount,
        agg_feif.item_fee__currency_code as actual_amazon_fee_currency_code

    from joined_orders_with_added_fields as jo_w_af

    left join aggregated_financial_events_item_fees as agg_feif
        on
            jo_w_af.amazon_order_id = agg_feif.amazon_order_id
            and jo_w_af.order_item_id = agg_feif.order_item_id
            and jo_w_af.seller_sku = agg_feif.seller_sku
            and jo_w_af.marketplace = agg_feif.marketplace
            and jo_w_af.tenant_id = agg_feif.tenant_id

)

select * from get_actual_amazon_fees
