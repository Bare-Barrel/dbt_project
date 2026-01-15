-- int_calculate_fields_for_aggregated_joined_orders.sql 07

{{ config(materialized='view') }}

with

agg_joined_orders as (

    select * from {{ ref('int_aggregate_joined_orders_with_added_fields') }}

),

rymora_product_md as (

    select
        current_sku,
        previous_sku,
        old_sku

    from {{ ref('stg_google_sheets__rymora_product_md') }}

),

match_old_sku_to_current as (

    select
        agg_jo.purchase_date,
        agg_jo.marketplace,
        agg_jo.order_status,
        agg_jo.asin,
        agg_jo.seller_sku,
        agg_jo.item_price_currency_code,
        agg_jo.is_vine,
        agg_jo.is_replacement_order,
        agg_jo.tenant_id,
        agg_jo.product_code,
        agg_jo.quantity_ordered,
        agg_jo.item_price_amount,
        agg_jo.promotion_discount_amount,
        agg_jo.item_tax_amount,
        agg_jo.shipping_price_amount,
        agg_jo.shipping_discount_amount,
        agg_jo.buyer_info_gift_wrap_price_amount,
        agg_jo.output_vat,
        agg_jo.coupon_fee,
        agg_jo.actual_amazon_fee_amount_usd,

        -- new_sku to handle old Rymora skus
        case
            when REGEXP_CONTAINS(agg_jo.seller_sku, r'^RMA-SPO.*')
                then ry_p_md.current_sku
        end as new_sku

    from agg_joined_orders as agg_jo

    left join rymora_product_md as ry_p_md
        on agg_jo.seller_sku = ry_p_md.previous_sku

),

calculate_fields as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        new_sku,
        item_price_currency_code,
        is_vine,
        is_replacement_order,
        tenant_id,
        product_code,
        quantity_ordered,
        item_price_amount,
        promotion_discount_amount,
        item_tax_amount,
        shipping_price_amount,
        shipping_discount_amount,
        buyer_info_gift_wrap_price_amount,
        output_vat,
        coupon_fee,
        actual_amazon_fee_amount_usd,

        -- Sales Price per unit, UK Sales have 20% VAT included
        case
            when marketplace = 'UK'
                then
                    ROUND(
                        SAFE_DIVIDE(item_price_amount - promotion_discount_amount - coupon_fee - output_vat, quantity_ordered
                        ), 2
                    )
            else
                ROUND(
                    SAFE_DIVIDE(item_price_amount - promotion_discount_amount - coupon_fee, quantity_ordered
                    ), 2
                )
        end as net_item_price_per_unit,

        -- Sales Price per order, UK Sales have 20% VAT included
        case
            when marketplace = 'UK'
                then
                    item_price_amount - promotion_discount_amount - coupon_fee - output_vat
            else
                item_price_amount - promotion_discount_amount - coupon_fee
        end as net_item_price_amount,

        -- Estimated Referral Fee Percentage
        case
            when tenant_id = 1
                then 0.15
            when tenant_id = 2
                then
                    case
                        when
                            REGEXP_CONTAINS(seller_sku, r'^R_CALF-SLEEV.*') -- Calf Sleeves
                            or REGEXP_CONTAINS(seller_sku, r'^R_GRIP-SOCKS.*') -- Grip Socks
                            or REGEXP_CONTAINS(seller_sku, r'^R_COMP-SOCKS.*') -- Compression Socks
                            or REGEXP_CONTAINS(seller_sku, r'^R_PF-SOCKS.*') -- Plantar Socks
                            then
                                case
                                    when marketplace = 'US'
                                        then 0.05
                                    when marketplace = 'UK'
                                        then 0.08
                                end
                        when (
                            REGEXP_CONTAINS(seller_sku, r'^R_KNEE-SLE.*') -- Knee Sleeves
                            or REGEXP_CONTAINS(seller_sku, r'^R_ELBO-SLE.*') -- Elbow Sleeves
                        )
                        and (
                            marketplace = 'US'
                            or marketplace = 'UK'
                        )
                            then
                                case
                                    when REGEXP_CONTAINS(seller_sku, r'^.*_2PC_.*')
                                        then 0.15
                                    else 0.08
                                end
                        when
                            REGEXP_CONTAINS(seller_sku, r'^R_HIKE-SOC.*') -- Merino Wool Hiking Socks
                            and marketplace = 'UK'
                            then
                                case
                                    when REGEXP_CONTAINS(seller_sku, r'^.*_3PR_.*')
                                        then 0.15
                                    else 0.08
                                end
                        when REGEXP_CONTAINS(seller_sku, r'^RMA-SPO*')
                            then
                                case
                                    when
                                        REGEXP_CONTAINS(new_sku, r'^R_CALF-SLEEV.*') -- Calf Sleeves
                                        or REGEXP_CONTAINS(new_sku, r'^R_GRIP-SOCKS.*') -- Grip Socks
                                        or REGEXP_CONTAINS(new_sku, r'^R_COMP-SOCKS.*') -- Compression Socks
                                        or REGEXP_CONTAINS(new_sku, r'^R_PF-SOCKS.*') -- Plantar Socks
                                        then
                                            case
                                                when marketplace = 'US'
                                                    then 0.05
                                                when marketplace = 'UK'
                                                    then 0.08
                                            end
                                    when (
                                        REGEXP_CONTAINS(new_sku, r'^R_KNEE-SLE.*') -- Knee Sleeves
                                        or REGEXP_CONTAINS(new_sku, r'^R_ELBO-SLE.*') -- Elbow Sleeves
                                    )
                                    and (
                                        marketplace = 'US'
                                        or marketplace = 'UK'
                                    )
                                        then
                                            case
                                                when REGEXP_CONTAINS(new_sku, r'^.*_2PC_.*')
                                                    then 0.15
                                                else 0.08
                                            end
                                    when
                                        REGEXP_CONTAINS(new_sku, r'^R_HIKE-SOC.*') -- Merino Wool Hiking Socks
                                        and marketplace = 'UK'
                                        then
                                            case
                                                when REGEXP_CONTAINS(new_sku, r'^.*_3PR_.*')
                                                    then 0.15
                                                else 0.08
                                            end
                                end
                    end
        end as referral_fee_pct

    from match_old_sku_to_current

),

add_referral_fees as (

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
        product_code,
        quantity_ordered,
        item_price_amount,
        promotion_discount_amount,
        item_tax_amount,
        output_vat,
        coupon_fee,
        net_item_price_per_unit,
        net_item_price_amount,
        actual_amazon_fee_amount_usd,
        CAST(referral_fee_pct as numeric) as referral_fee_pct,
        CAST(CAST(referral_fee_pct as numeric) * (COALESCE(net_item_price_amount, 0) + COALESCE(shipping_price_amount, 0) - COALESCE(shipping_discount_amount, 0) + COALESCE(buyer_info_gift_wrap_price_amount, 0)) as numeric)
            as referral_fees

    from calculate_fields

)

select * from add_referral_fees
