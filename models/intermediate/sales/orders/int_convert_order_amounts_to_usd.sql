-- int_convert_order_amounts_to_usd.sql 06

{{ config(materialized='view') }}

with

joined_orders_with_cogs as (

    select * from {{ ref('int_get_cogs_fba_storage_returns_fees_for_joined_orders') }}

),

fx as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

convert_amounts_to_usd as (

    select
        j_o_w_c.amazon_order_id,
        j_o_w_c.order_item_id,
        j_o_w_c.purchase_datetime,
        j_o_w_c.purchase_date,
        j_o_w_c.marketplace,
        j_o_w_c.sales_channel,
        j_o_w_c.asin,
        j_o_w_c.seller_sku,
        j_o_w_c.order_status,
        j_o_w_c.quantity_ordered,
        j_o_w_c.promotion_ids,
        j_o_w_c.product_info_number_of_items,
        j_o_w_c.tax_collection_model,
        j_o_w_c.tax_collection_responsible_party,
        j_o_w_c.is_prime,
        j_o_w_c.is_replacement_order,
        j_o_w_c.replaced_order_id,
        j_o_w_c.is_gift,
        j_o_w_c.is_vine,
        j_o_w_c.tenant_id,

        -- Amounts
        {# j_o_w_c.item_price_currency_code,
        j_o_w_c.item_price_amount,
        j_o_w_c.coupon_fee,
        j_o_w_c.item_tax_currency_code,
        j_o_w_c.item_tax_amount,
        j_o_w_c.uk_output_vat,
        j_o_w_c.promotion_discount_currency_code,
        j_o_w_c.promotion_discount_amount,
        j_o_w_c.promotion_discount_tax_currency_code,
        j_o_w_c.promotion_discount_tax_amount,
        j_o_w_c.shipping_price_currency_code,
        j_o_w_c.shipping_price_amount, -- !!many nulls!!
        j_o_w_c.shipping_discount_currency_code,
        j_o_w_c.shipping_discount_amount, -- !!many nulls!!
        j_o_w_c.buyer_info_gift_wrap_price_currency_code,
        j_o_w_c.buyer_info_gift_wrap_price_amount, -- !!many nulls!!
        j_o_w_c.est_fees_currency_code,
        j_o_w_c.cogs,
        j_o_w_c.est_fba_fee,
        j_o_w_c.est_storage_fee,
        j_o_w_c.est_returns_cost, #}

        -- Item price
        case
            when j_o_w_c.item_price_currency_code = "USD"
                then j_o_w_c.item_price_amount
            else SAFE_DIVIDE(j_o_w_c.item_price_amount, fx_item.rate)
        end as item_price_amount_usd,
        case
            when j_o_w_c.item_price_currency_code = "USD"
                then j_o_w_c.coupon_fee
            else SAFE_DIVIDE(j_o_w_c.coupon_fee, fx_item.rate)
        end as coupon_fee_usd,

        -- Item tax
        case
            when j_o_w_c.item_tax_currency_code = "USD"
                then j_o_w_c.item_tax_amount
            else SAFE_DIVIDE(j_o_w_c.item_tax_amount, fx_item_tax.rate)
        end as item_tax_amount_usd,
        case
            when j_o_w_c.item_tax_currency_code = "USD"
                then j_o_w_c.uk_output_vat
            else SAFE_DIVIDE(j_o_w_c.uk_output_vat, fx_item_tax.rate)
        end as uk_output_vat_usd,

        -- Promotion discount
        case
            when j_o_w_c.promotion_discount_currency_code = "USD"
                then j_o_w_c.promotion_discount_amount
            else SAFE_DIVIDE(j_o_w_c.promotion_discount_amount, fx_promo_discount.rate)
        end as promotion_discount_amount_usd,

        -- Promotion discount tax
        case
            when j_o_w_c.promotion_discount_tax_currency_code = "USD"
                then j_o_w_c.promotion_discount_tax_amount
            else SAFE_DIVIDE(j_o_w_c.promotion_discount_tax_amount, fx_promo_discount_tax.rate)
        end as promotion_discount_tax_amount_usd,

        -- Shipping price
        case
            when j_o_w_c.shipping_price_currency_code = "USD"
                then j_o_w_c.shipping_price_amount
            else SAFE_DIVIDE(j_o_w_c.shipping_price_amount, fx_shipping.rate)
        end as shipping_price_amount_usd,

        -- Shipping discount
        case
            when j_o_w_c.shipping_discount_currency_code = "USD"
                then j_o_w_c.shipping_discount_amount
            else SAFE_DIVIDE(j_o_w_c.shipping_discount_amount, fx_shipping_discount.rate)
        end as shipping_discount_amount_usd,

        -- Gift wrap
        case
            when j_o_w_c.buyer_info_gift_wrap_price_currency_code = "USD"
                then j_o_w_c.buyer_info_gift_wrap_price_amount
            else SAFE_DIVIDE(j_o_w_c.buyer_info_gift_wrap_price_amount, fx_gift.rate)
        end as buyer_info_gift_wrap_price_amount_usd,

        -- Est. fees
        case
            when j_o_w_c.est_fees_currency_code = "USD"
                then j_o_w_c.cogs
            else SAFE_DIVIDE(j_o_w_c.cogs, fx_est_fees.rate)
        end as cogs_usd,
        case
            when j_o_w_c.est_fees_currency_code = "USD"
                then j_o_w_c.est_fba_fee
            else SAFE_DIVIDE(j_o_w_c.est_fba_fee, fx_est_fees.rate)
        end as est_fba_fee_usd,
        case
            when j_o_w_c.est_fees_currency_code = "USD"
                then j_o_w_c.est_storage_fee
            else SAFE_DIVIDE(j_o_w_c.est_storage_fee, fx_est_fees.rate)
        end as est_storage_fee_usd,
        case
            when j_o_w_c.est_fees_currency_code = "USD"
                then j_o_w_c.est_returns_cost
            else SAFE_DIVIDE(j_o_w_c.est_returns_cost, fx_est_fees.rate)
        end as est_returns_cost_usd

    from joined_orders_with_cogs as j_o_w_c

    -- join once per DISTINCT currency column
    left join fx as fx_item
        on
            j_o_w_c.purchase_date = fx_item.recorded_at
            and j_o_w_c.item_price_currency_code = fx_item.target

    left join fx as fx_item_tax
        on
            j_o_w_c.purchase_date = fx_item_tax.recorded_at
            and j_o_w_c.item_tax_currency_code = fx_item_tax.target

    left join fx as fx_promo_discount
        on
            j_o_w_c.purchase_date = fx_promo_discount.recorded_at
            and j_o_w_c.promotion_discount_currency_code = fx_promo_discount.target

    left join fx as fx_promo_discount_tax
        on
            j_o_w_c.purchase_date = fx_promo_discount_tax.recorded_at
            and j_o_w_c.promotion_discount_tax_currency_code = fx_promo_discount_tax.target

    left join fx as fx_shipping
        on
            j_o_w_c.purchase_date = fx_shipping.recorded_at
            and j_o_w_c.shipping_price_currency_code = fx_shipping.target

    left join fx as fx_shipping_discount
        on
            j_o_w_c.purchase_date = fx_shipping_discount.recorded_at
            and j_o_w_c.shipping_discount_currency_code = fx_shipping_discount.target

    left join fx as fx_gift
        on
            j_o_w_c.purchase_date = fx_gift.recorded_at
            and j_o_w_c.buyer_info_gift_wrap_price_currency_code = fx_gift.target

    left join fx as fx_est_fees
        on
            j_o_w_c.purchase_date = fx_est_fees.recorded_at
            and j_o_w_c.est_fees_currency_code = fx_est_fees.target

)

select * from convert_amounts_to_usd
