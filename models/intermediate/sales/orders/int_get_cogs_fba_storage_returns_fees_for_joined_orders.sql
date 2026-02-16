-- int_get_cogs_fba_storage_returns_fees_for_joined_orders.sql 05

{{ config(materialized='ephemeral') }}

with

joined_orders_with_added_fields as (

    select * from {{ ref('int_add_fields_to_joined_orders') }}

),

combined_cogs_ww as (

    select * from {{ ref('int_combine_cogs_ww') }}

),

get_cogs_fba_returns_fees as (

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
        jo_w_af.uk_output_vat,
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
        c_cogs.currency_code as est_fees_currency_code,
        c_cogs.cogs as cogs_per_order_item,
        c_cogs.fba_fee as est_fba_fee_per_order_item,
        c_cogs.storage_fee as est_storage_fee_per_order_item,
        c_cogs.returns_cost as est_returns_cost_per_order_item

    from joined_orders_with_added_fields as jo_w_af

    left join combined_cogs_ww as c_cogs
        on
            jo_w_af.asin = c_cogs.asin
            and jo_w_af.marketplace = c_cogs.marketplace
            and jo_w_af.tenant_id = c_cogs.tenant_id
            and jo_w_af.purchase_date between c_cogs.start_date and c_cogs.end_date

    qualify ROW_NUMBER() over (
        partition by jo_w_af.order_item_id
        order by c_cogs.start_date desc
    ) = 1

),

compute_total_est_fees as (

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

        cogs_per_order_item * quantity_ordered as cogs,
        est_fba_fee_per_order_item * quantity_ordered as est_fba_fee,
        est_storage_fee_per_order_item * quantity_ordered as est_storage_fee,
        est_returns_cost_per_order_item * quantity_ordered as est_returns_cost

    from get_cogs_fba_returns_fees

)

select * from compute_total_est_fees
