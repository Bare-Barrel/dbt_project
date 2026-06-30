-- int_get_cogs_fba_storage_returns_fees_for_joined_orders.sql 06

{{ config(materialized='ephemeral') }}

with

joined_orders_with_local_tz as (

    select * from {{ ref('int_convert_datetime_to_marketplace_tz') }}

),

combined_cogs_ww as (

    select * from {{ ref('int_combine_cogs_ww') }}

),

get_cogs_fba_returns_fees as (

    select
        jo_w_ltz.amazon_order_id,
        jo_w_ltz.order_item_id,
        jo_w_ltz.purchase_datetime_utc,
        jo_w_ltz.purchase_date_utc,
        jo_w_ltz.purchase_datetime_local,
        jo_w_ltz.purchase_date_local,
        jo_w_ltz.marketplace,
        jo_w_ltz.sales_channel,
        jo_w_ltz.asin,
        jo_w_ltz.seller_sku,
        jo_w_ltz.order_status,
        jo_w_ltz.quantity_ordered,
        jo_w_ltz.promotion_ids,
        jo_w_ltz.product_info_number_of_items,
        jo_w_ltz.item_price_currency_code,
        jo_w_ltz.item_price_amount,
        jo_w_ltz.item_tax_amount,
        jo_w_ltz.promotion_discount_tax_currency_code,
        jo_w_ltz.uk_output_vat,
        jo_w_ltz.item_tax_currency_code,
        jo_w_ltz.promotion_discount_tax_amount,
        jo_w_ltz.promotion_discount_currency_code,
        jo_w_ltz.promotion_discount_amount,
        jo_w_ltz.coupon_fee,
        jo_w_ltz.tax_collection_model,
        jo_w_ltz.tax_collection_responsible_party,
        jo_w_ltz.is_prime,
        jo_w_ltz.is_replacement_order,
        jo_w_ltz.replaced_order_id,
        jo_w_ltz.is_gift,
        jo_w_ltz.is_vine,
        jo_w_ltz.tenant_id,
        jo_w_ltz.shipping_price_amount,
        jo_w_ltz.shipping_price_currency_code,
        jo_w_ltz.shipping_discount_amount,
        jo_w_ltz.shipping_discount_currency_code,
        jo_w_ltz.buyer_info_gift_wrap_price_amount,
        jo_w_ltz.buyer_info_gift_wrap_price_currency_code,
        c_cogs.currency_code as est_fees_currency_code,
        c_cogs.cogs as cogs_per_order_item,
        c_cogs.fba_fee as est_fba_fee_per_order_item,
        c_cogs.storage_fee as est_storage_fee_per_order_item,
        c_cogs.returns_cost as est_returns_cost_per_order_item,
        c_cogs.referral_rate as est_referral_rate

    from joined_orders_with_local_tz as jo_w_ltz

    left join combined_cogs_ww as c_cogs
        on
            jo_w_ltz.asin = c_cogs.asin
            and jo_w_ltz.marketplace = c_cogs.marketplace
            and jo_w_ltz.tenant_id = c_cogs.tenant_id
            and jo_w_ltz.purchase_date_local between c_cogs.start_date and c_cogs.end_date

    qualify ROW_NUMBER() over (
        partition by jo_w_ltz.order_item_id
        order by c_cogs.start_date desc
    ) = 1

),

compute_total_est_fees as (

    select
        amazon_order_id,
        order_item_id,
        purchase_datetime_utc,
        purchase_date_utc,
        purchase_datetime_local,
        purchase_date_local,
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
        est_fees_currency_code,
        est_referral_rate,

        cogs_per_order_item * quantity_ordered as cogs,
        est_fba_fee_per_order_item * quantity_ordered as est_fba_fee,
        est_storage_fee_per_order_item * quantity_ordered as est_storage_fee,
        est_returns_cost_per_order_item * quantity_ordered as est_returns_cost

    from get_cogs_fba_returns_fees

)

select * from compute_total_est_fees
