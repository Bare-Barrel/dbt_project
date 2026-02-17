-- int_aggregate_joined_orders_with_added_fields.sql 11

{{ config(materialized='ephemeral') }}

with

joined_orders_with_final_amazon_fees as (

    select * from {{ ref('int_calculate_final_amazon_fees_for_joined_orders') }}

),

aggregate_joined_orders_with_added_fields as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        is_vine,
        is_replacement_order,
        tenant_id,

        SUM(quantity_ordered) as quantity_ordered,
        SUM(item_price_amount_usd) as item_price_amount_usd,
        SUM(net_item_price_amount_usd) as net_item_price_amount_usd,
        SUM(coupon_fee_usd) as coupon_fee_usd,
        SUM(item_tax_amount_usd) as item_tax_amount_usd,
        SUM(uk_output_vat_usd) as uk_output_vat_usd,
        SUM(promotion_discount_amount_usd) as promotion_discount_amount_usd,
        SUM(shipping_price_amount_usd) as shipping_price_amount_usd,
        SUM(shipping_discount_amount_usd) as shipping_discount_amount_usd,
        SUM(buyer_info_gift_wrap_price_amount_usd) as buyer_info_gift_wrap_price_amount_usd,
        SUM(cogs_usd) as cogs_usd,
        {# SUM(est_fba_fee_usd) as est_fba_fee_usd, #}
        SUM(est_storage_fee_usd) as est_storage_fee_usd,
        SUM(est_returns_cost_usd) as est_returns_cost_usd,
        {# SUM(est_referral_fee_usd) as est_referral_fee_usd, #}
        {# SUM(est_amazon_fees_usd) as est_amazon_fees_usd, #}
        {# SUM(actual_amazon_fees_usd) as actual_amazon_fees_usd, #}
        SUM(amazon_fees_usd) as amazon_fees_usd

    from joined_orders_with_final_amazon_fees

    group by
        purchase_date,
        marketplace,
        asin,
        seller_sku,
        is_vine,
        is_replacement_order,
        tenant_id,
        order_status

    order by purchase_date desc, marketplace desc, quantity_ordered desc

)

select * from aggregate_joined_orders_with_added_fields
