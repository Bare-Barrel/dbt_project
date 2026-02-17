-- int_get_est_referral_fees_for_joined_orders.sql 07

{{ config(materialized='view') }}

with

orders_with_rymora_sku_matches as (

    select * from {{ ref('int_match_old_rymora_skus_for_joined_orders') }}

),

get_est_referral_fees as (

    select
        amazon_order_id,
        order_item_id,
        purchase_datetime,
        purchase_date,
        marketplace,
        sales_channel,
        asin,
        seller_sku,
        new_sku,
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
        cogs,
        est_fba_fee,
        est_storage_fee,
        est_returns_cost,

        -- Estimated Referral Fee Percentage
        case
            when tenant_id = 1
                then 0.15
            when tenant_id = 2
                then
                    case
                        when marketplace = 'CA'
                            then 0.15
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
                        when REGEXP_CONTAINS(seller_sku, r'^RMA-SPO.*')
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
        end as est_referral_fee_pct

    from orders_with_rymora_sku_matches

),

compute_net_item_price_amount as (

    select
        *,

        -- Sales Price per order, UK Sales have 20% VAT included
        case
            when marketplace = 'UK'
                then
                    item_price_amount - promotion_discount_amount - coupon_fee - uk_output_vat
            else
                item_price_amount - promotion_discount_amount - coupon_fee
        end as net_item_price_amount

    from get_est_referral_fees

),

compute_est_referral_fee as (

    select
        amazon_order_id,
        order_item_id,
        purchase_datetime,
        purchase_date,
        marketplace,
        sales_channel,
        asin,
        seller_sku,
        new_sku,
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
        cogs,
        est_fba_fee,
        est_storage_fee,
        est_returns_cost,
        net_item_price_amount,

        CAST(est_referral_fee_pct as numeric) as est_referral_fee_pct,
        CAST(CAST(est_referral_fee_pct as numeric) * (COALESCE(net_item_price_amount, 0) + COALESCE(shipping_price_amount, 0) - COALESCE(shipping_discount_amount, 0) + COALESCE(buyer_info_gift_wrap_price_amount, 0)) as numeric)
            as est_referral_fee

    from compute_net_item_price_amount

)

select * from compute_est_referral_fee
