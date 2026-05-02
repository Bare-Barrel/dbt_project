-- int_calculate_fields_for_aggregated_joined_orders.sql 12

{{ config(materialized='view') }}

with

agg_joined_orders as (

    select * from {{ ref('int_aggregate_joined_orders_with_added_fields') }}

),

calculate_fields as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        {# new_sku, #}
        is_vine,
        is_replacement_order,
        tenant_id,
        quantity_ordered,
        item_price_amount_usd,
        net_item_price_amount_usd,
        coupon_fee_usd,
        item_tax_amount_usd,
        uk_output_vat_usd,
        promotion_discount_amount_usd,
        shipping_price_amount_usd,
        shipping_discount_amount_usd,
        buyer_info_gift_wrap_price_amount_usd,
        cogs_usd,
        {# est_fba_fee_usd, #}
        est_storage_fee_usd,
        est_returns_cost_usd,
        {# est_referral_fee_usd, #}
        {# est_amazon_fees_usd, #}
        {# actual_amazon_fees_usd, #}

        -- Product Code
        REGEXP_EXTRACT(seller_sku, r'[^_]+$') as product_code,

        -- Absolute value of Amazon Fees
        ABS(amazon_fees_usd) as amazon_fees_usd,

        -- Sales Price per unit, UK Sales have 20% VAT included
        case
            when marketplace = 'UK'
                then
                    ROUND(
                        SAFE_DIVIDE(item_price_amount_usd - promotion_discount_amount_usd - coupon_fee_usd - uk_output_vat_usd, quantity_ordered
                        ), 2
                    )
            else
                ROUND(
                    SAFE_DIVIDE(item_price_amount_usd - promotion_discount_amount_usd - coupon_fee_usd, quantity_ordered
                    ), 2
                )
        end as net_item_price_per_unit_usd

    from agg_joined_orders

)

select * from calculate_fields
