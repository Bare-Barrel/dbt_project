-- int_calculate_final_amazon_fees_for_joined_orders.sql 10 -- Compare est and actual amazon fees on order item level

{{ config(materialized='view') }}

with

orders_with_est_and_actual_amazon_fees as (

    select * from {{ ref('int_get_actual_amazon_fees_of_joined_orders') }}

),

compute_est_amazon_fees as (

    select
        *,

        est_fba_fee_usd + est_referral_fee_usd as est_amazon_fees_usd

    from orders_with_est_and_actual_amazon_fees

),

get_final_amazon_fees as (

    select
        amazon_order_id,
        order_item_id,
        purchase_datetime,
        purchase_date,
        marketplace,
        sales_channel,
        asin,
        seller_sku,
        order_status,
        quantity_ordered,
        promotion_ids,
        product_info_number_of_items,
        tax_collection_model,
        tax_collection_responsible_party,
        is_prime,
        is_replacement_order,
        replaced_order_id,
        is_gift,
        is_vine,
        tenant_id,
        item_price_amount_usd,
        net_item_price_amount_usd,
        coupon_fee_usd,
        item_tax_amount_usd,
        uk_output_vat_usd,
        promotion_discount_amount_usd,
        promotion_discount_tax_amount_usd,
        shipping_price_amount_usd,
        shipping_discount_amount_usd,
        buyer_info_gift_wrap_price_amount_usd,
        cogs_usd,
        est_fba_fee_usd,
        est_storage_fee_usd,
        est_returns_cost_usd,
        est_referral_fee_usd,
        est_amazon_fees_usd,
        actual_amazon_fees_usd,

        -- Final Amazon Fees
        COALESCE(actual_amazon_fees_usd, est_amazon_fees_usd) as amazon_fees_usd

    from compute_est_amazon_fees

)

select * from get_final_amazon_fees
