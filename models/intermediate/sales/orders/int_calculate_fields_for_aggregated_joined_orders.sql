-- int_calculate_fields_for_aggregated_joined_orders.sql 09

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

derive_product_code as (

    select
        *,

        REGEXP_EXTRACT(seller_sku, r'[^_]+$') as product_code

    from agg_joined_orders

),

match_old_sku_to_current as (

    select
        d_p_c.purchase_date,
        d_p_c.marketplace,
        d_p_c.order_status,
        d_p_c.asin,
        d_p_c.seller_sku,
        d_p_c.is_vine,
        d_p_c.is_replacement_order,
        d_p_c.tenant_id,
        d_p_c.product_code,
        d_p_c.quantity_ordered,
        d_p_c.item_price_amount_usd,
        d_p_c.coupon_fee_usd,
        d_p_c.item_tax_amount_usd,
        d_p_c.uk_output_vat_usd,
        d_p_c.promotion_discount_amount_usd,
        d_p_c.shipping_price_amount_usd,
        d_p_c.shipping_discount_amount_usd,
        d_p_c.buyer_info_gift_wrap_price_amount_usd,
        d_p_c.cogs_usd,
        d_p_c.est_fba_fee_usd,
        d_p_c.est_storage_fee_usd,
        d_p_c.est_returns_cost_usd,
        d_p_c.actual_amazon_fees_usd,

        -- new_sku to handle old Rymora skus
        case
            when REGEXP_CONTAINS(d_p_c.seller_sku, r'^RMA-SPO.*')
                then ry_p_md.current_sku
        end as new_sku

    from derive_product_code as d_p_c

    left join rymora_product_md as ry_p_md
        on d_p_c.seller_sku = ry_p_md.previous_sku

),

match_old_sku_2_to_current as (     -- match the second time around

    select
        m_o_sku.purchase_date,
        m_o_sku.marketplace,
        m_o_sku.order_status,
        m_o_sku.asin,
        m_o_sku.seller_sku,
        m_o_sku.is_vine,
        m_o_sku.is_replacement_order,
        m_o_sku.tenant_id,
        m_o_sku.product_code,
        m_o_sku.quantity_ordered,
        m_o_sku.item_price_amount_usd,
        m_o_sku.coupon_fee_usd,
        m_o_sku.item_tax_amount_usd,
        m_o_sku.uk_output_vat_usd,
        m_o_sku.promotion_discount_amount_usd,
        m_o_sku.shipping_price_amount_usd,
        m_o_sku.shipping_discount_amount_usd,
        m_o_sku.buyer_info_gift_wrap_price_amount_usd,
        m_o_sku.cogs_usd,
        m_o_sku.est_fba_fee_usd,
        m_o_sku.est_storage_fee_usd,
        m_o_sku.est_returns_cost_usd,
        m_o_sku.actual_amazon_fees_usd,

        -- new_sku to also handle old_2 Rymora skus
        case
            when
                REGEXP_CONTAINS(m_o_sku.seller_sku, r'^RMA-SPO.*')
                and m_o_sku.new_sku is null
                then ry_p_md.current_sku
            when
                REGEXP_CONTAINS(m_o_sku.seller_sku, r'^RMA-SPO.*')
                and m_o_sku.new_sku is not null
                then m_o_sku.new_sku
        end as new_sku

    from match_old_sku_to_current as m_o_sku

    left join rymora_product_md as ry_p_md
        on m_o_sku.seller_sku = ry_p_md.old_sku

),

calculate_fields as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        new_sku,
        is_vine,
        is_replacement_order,
        tenant_id,
        product_code,
        quantity_ordered,
        item_price_amount_usd,
        coupon_fee_usd,
        item_tax_amount_usd,
        uk_output_vat_usd,
        promotion_discount_amount_usd,
        shipping_price_amount_usd,
        shipping_discount_amount_usd,
        buyer_info_gift_wrap_price_amount_usd,
        cogs_usd,
        est_fba_fee_usd,
        est_storage_fee_usd,
        est_returns_cost_usd,
        actual_amazon_fees_usd,

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
        end as net_item_price_per_unit_usd,

        -- Sales Price per order, UK Sales have 20% VAT included
        case
            when marketplace = 'UK'
                then
                    item_price_amount_usd - promotion_discount_amount_usd - coupon_fee_usd - uk_output_vat_usd
            else
                item_price_amount_usd - promotion_discount_amount_usd - coupon_fee_usd
        end as net_item_price_amount_usd,

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

    from match_old_sku_2_to_current

),

compute_est_referral_fee as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        new_sku,
        is_vine,
        is_replacement_order,
        tenant_id,
        product_code,
        quantity_ordered,
        item_price_amount_usd,
        coupon_fee_usd,
        item_tax_amount_usd,
        uk_output_vat_usd,
        promotion_discount_amount_usd,
        shipping_price_amount_usd,
        shipping_discount_amount_usd,
        buyer_info_gift_wrap_price_amount_usd,
        cogs_usd,
        est_fba_fee_usd,
        est_storage_fee_usd,
        est_returns_cost_usd,
        net_item_price_per_unit_usd,
        net_item_price_amount_usd,

        ABS(actual_amazon_fees_usd) as actual_amazon_fees_usd,
        CAST(est_referral_fee_pct as numeric) as est_referral_fee_pct,
        CAST(CAST(est_referral_fee_pct as numeric) * (COALESCE(net_item_price_amount_usd, 0) + COALESCE(shipping_price_amount_usd, 0) - COALESCE(shipping_discount_amount_usd, 0) + COALESCE(buyer_info_gift_wrap_price_amount_usd, 0)) as numeric)
            as est_referral_fee_usd

    from calculate_fields

),

compute_est_amazon_fees as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        new_sku,
        is_vine,
        is_replacement_order,
        tenant_id,
        product_code,
        quantity_ordered,
        item_price_amount_usd,
        coupon_fee_usd,
        item_tax_amount_usd,
        uk_output_vat_usd,
        promotion_discount_amount_usd,
        shipping_price_amount_usd,
        shipping_discount_amount_usd,
        buyer_info_gift_wrap_price_amount_usd,
        cogs_usd,
        est_fba_fee_usd,
        est_storage_fee_usd,
        est_returns_cost_usd,
        net_item_price_per_unit_usd,
        net_item_price_amount_usd,
        actual_amazon_fees_usd,
        est_referral_fee_pct,
        est_referral_fee_usd,

        est_fba_fee_usd + est_referral_fee_usd as est_amazon_fees_usd

    from compute_est_referral_fee

)

select * from compute_est_amazon_fees
