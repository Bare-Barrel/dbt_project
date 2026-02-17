-- int_convert_order_amounts_to_usd.sql 08

{{ config(materialized='view') }}

with

order_with_est_fees as (

    select * from {{ ref('int_get_est_referral_fees_for_joined_orders') }}

),

fx as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

convert_amounts_to_usd as (

    select
        o_w_ef.amazon_order_id,
        o_w_ef.order_item_id,
        o_w_ef.purchase_datetime,
        o_w_ef.purchase_date,
        o_w_ef.marketplace,
        o_w_ef.sales_channel,
        o_w_ef.asin,
        o_w_ef.seller_sku,
        o_w_ef.order_status,
        o_w_ef.quantity_ordered,
        o_w_ef.promotion_ids,
        o_w_ef.product_info_number_of_items,
        o_w_ef.tax_collection_model,
        o_w_ef.tax_collection_responsible_party,
        o_w_ef.is_prime,
        o_w_ef.is_replacement_order,
        o_w_ef.replaced_order_id,
        o_w_ef.is_gift,
        o_w_ef.is_vine,
        o_w_ef.tenant_id,

        -- Amounts
        {# o_w_ef.item_price_currency_code,
        o_w_ef.item_price_amount,
        o_w_ef.coupon_fee,
        o_w_ef.item_tax_currency_code,
        o_w_ef.item_tax_amount,
        o_w_ef.uk_output_vat,
        o_w_ef.promotion_discount_currency_code,
        o_w_ef.promotion_discount_amount,
        o_w_ef.promotion_discount_tax_currency_code,
        o_w_ef.promotion_discount_tax_amount,
        o_w_ef.shipping_price_currency_code,
        o_w_ef.shipping_price_amount, -- !!many nulls!!
        o_w_ef.shipping_discount_currency_code,
        o_w_ef.shipping_discount_amount, -- !!many nulls!!
        o_w_ef.buyer_info_gift_wrap_price_currency_code,
        o_w_ef.buyer_info_gift_wrap_price_amount, -- !!many nulls!!
        o_w_ef.est_fees_currency_code,
        o_w_ef.cogs,
        o_w_ef.est_fba_fee,
        o_w_ef.est_storage_fee,
        o_w_ef.est_returns_cost, 
        o_w_ef.est_referral_fee,
        o_w_ef.net_item_price_amount, #}

        -- Item price
        case
            when o_w_ef.item_price_currency_code = "USD"
                then o_w_ef.item_price_amount
            else SAFE_DIVIDE(o_w_ef.item_price_amount, fx_item.rate)
        end as item_price_amount_usd,
        case
            when o_w_ef.item_price_currency_code = "USD"
                then o_w_ef.net_item_price_amount
            else SAFE_DIVIDE(o_w_ef.net_item_price_amount, fx_item.rate)
        end as net_item_price_amount_usd,
        case
            when o_w_ef.item_price_currency_code = "USD"
                then o_w_ef.coupon_fee
            else SAFE_DIVIDE(o_w_ef.coupon_fee, fx_item.rate)
        end as coupon_fee_usd,

        -- Item tax
        case
            when o_w_ef.item_tax_currency_code = "USD"
                then o_w_ef.item_tax_amount
            else SAFE_DIVIDE(o_w_ef.item_tax_amount, fx_item_tax.rate)
        end as item_tax_amount_usd,
        case
            when o_w_ef.item_tax_currency_code = "USD"
                then o_w_ef.uk_output_vat
            else SAFE_DIVIDE(o_w_ef.uk_output_vat, fx_item_tax.rate)
        end as uk_output_vat_usd,

        -- Promotion discount
        case
            when o_w_ef.promotion_discount_currency_code = "USD"
                then o_w_ef.promotion_discount_amount
            else SAFE_DIVIDE(o_w_ef.promotion_discount_amount, fx_promo_discount.rate)
        end as promotion_discount_amount_usd,

        -- Promotion discount tax
        case
            when o_w_ef.promotion_discount_tax_currency_code = "USD"
                then o_w_ef.promotion_discount_tax_amount
            else SAFE_DIVIDE(o_w_ef.promotion_discount_tax_amount, fx_promo_discount_tax.rate)
        end as promotion_discount_tax_amount_usd,

        -- Shipping price
        case
            when o_w_ef.shipping_price_currency_code = "USD"
                then o_w_ef.shipping_price_amount
            else SAFE_DIVIDE(o_w_ef.shipping_price_amount, fx_shipping.rate)
        end as shipping_price_amount_usd,

        -- Shipping discount
        case
            when o_w_ef.shipping_discount_currency_code = "USD"
                then o_w_ef.shipping_discount_amount
            else SAFE_DIVIDE(o_w_ef.shipping_discount_amount, fx_shipping_discount.rate)
        end as shipping_discount_amount_usd,

        -- Gift wrap
        case
            when o_w_ef.buyer_info_gift_wrap_price_currency_code = "USD"
                then o_w_ef.buyer_info_gift_wrap_price_amount
            else SAFE_DIVIDE(o_w_ef.buyer_info_gift_wrap_price_amount, fx_gift.rate)
        end as buyer_info_gift_wrap_price_amount_usd,

        -- Est. fees
        case
            when o_w_ef.est_fees_currency_code = "USD"
                then o_w_ef.cogs
            else SAFE_DIVIDE(o_w_ef.cogs, fx_est_fees.rate)
        end as cogs_usd,
        case
            when o_w_ef.est_fees_currency_code = "USD"
                then o_w_ef.est_fba_fee
            else SAFE_DIVIDE(o_w_ef.est_fba_fee, fx_est_fees.rate)
        end as est_fba_fee_usd,
        case
            when o_w_ef.est_fees_currency_code = "USD"
                then o_w_ef.est_storage_fee
            else SAFE_DIVIDE(o_w_ef.est_storage_fee, fx_est_fees.rate)
        end as est_storage_fee_usd,
        case
            when o_w_ef.est_fees_currency_code = "USD"
                then o_w_ef.est_returns_cost
            else SAFE_DIVIDE(o_w_ef.est_returns_cost, fx_est_fees.rate)
        end as est_returns_cost_usd,
        case
            when o_w_ef.est_fees_currency_code = "USD"
                then o_w_ef.est_referral_fee
            else SAFE_DIVIDE(o_w_ef.est_referral_fee, fx_est_fees.rate)
        end as est_referral_fee_usd

    from order_with_est_fees as o_w_ef

    -- join once per DISTINCT currency column
    left join fx as fx_item
        on
            o_w_ef.purchase_date = fx_item.recorded_at
            and o_w_ef.item_price_currency_code = fx_item.target

    left join fx as fx_item_tax
        on
            o_w_ef.purchase_date = fx_item_tax.recorded_at
            and o_w_ef.item_tax_currency_code = fx_item_tax.target

    left join fx as fx_promo_discount
        on
            o_w_ef.purchase_date = fx_promo_discount.recorded_at
            and o_w_ef.promotion_discount_currency_code = fx_promo_discount.target

    left join fx as fx_promo_discount_tax
        on
            o_w_ef.purchase_date = fx_promo_discount_tax.recorded_at
            and o_w_ef.promotion_discount_tax_currency_code = fx_promo_discount_tax.target

    left join fx as fx_shipping
        on
            o_w_ef.purchase_date = fx_shipping.recorded_at
            and o_w_ef.shipping_price_currency_code = fx_shipping.target

    left join fx as fx_shipping_discount
        on
            o_w_ef.purchase_date = fx_shipping_discount.recorded_at
            and o_w_ef.shipping_discount_currency_code = fx_shipping_discount.target

    left join fx as fx_gift
        on
            o_w_ef.purchase_date = fx_gift.recorded_at
            and o_w_ef.buyer_info_gift_wrap_price_currency_code = fx_gift.target

    left join fx as fx_est_fees
        on
            o_w_ef.purchase_date = fx_est_fees.recorded_at
            and o_w_ef.est_fees_currency_code = fx_est_fees.target

)

select * from convert_amounts_to_usd
