

  create or replace view `modern-sublime-383117`.`dbt_cherry_intermediate`.`int_aggregate_joined_orders_with_added_fields`
  OPTIONS()
  as -- int_aggregate_joined_orders_with_added_fields.sql



with

joined_orders_with_added_fields as (

    select * from `modern-sublime-383117`.`dbt_cherry_intermediate`.`int_add_fields_to_joined_orders`

),

aggregate_joined_orders_with_added_fields as (

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
        REGEXP_EXTRACT(seller_sku, r'[^_]+$') as product_code,
        SUM(quantity_ordered) as quantity_ordered,
        SUM(item_price_amount) as item_price_amount,
        SUM(promotion_discount_amount) as promotion_discount_amount,
        SUM(item_tax_amount) as item_tax_amount,
        SUM(shipping_price_amount) as shipping_price_amount,
        SUM(shipping_discount_amount) as shipping_discount_amount,
        SUM(buyer_info_gift_wrap_price_amount) as buyer_info_gift_wrap_price_amount,
        SUM(output_vat) as output_vat,
        SUM(coupon_fee) as coupon_fee,

        -- Sales Price per unit, UK Sales have 20% VAT included
        case
            when marketplace = 'UK'
                then
                    ROUND(
                        SAFE_DIVIDE(SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee) - SUM(output_vat), SUM(quantity_ordered)
                        ), 2
                    )
            else
                ROUND(
                    SAFE_DIVIDE(SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee), SUM(quantity_ordered)
                    ), 2
                )
        end as net_item_price_per_unit,

        -- Sales Price per order, UK Sales have 20% VAT included
        case
            when marketplace = 'UK'
                then
                    SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee) - SUM(output_vat)
            else
                SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee)
        end as net_item_price_amount,

        -- Estimated Referral Fee Percentage
        case
            when tenant_id = 1
                then 0.15
            when tenant_id = 2
                then
                    case
                        when
                            REGEXP_CONTAINS(seller_sku, r'^R_CALF-SLEEV*') -- Calf Sleeves
                            or REGEXP_CONTAINS(seller_sku, r'^R_GRIP-SOCKS*') -- Grip Socks
                            or REGEXP_CONTAINS(seller_sku, r'^R_COMP-SOCKS*') -- Compression Socks
                            or REGEXP_CONTAINS(seller_sku, r'^R_PF-SOCKS*') -- Plantar Socks
                            then
                                case
                                    when marketplace = 'US'
                                        then 0.05
                                    when marketplace = 'UK'
                                        then 0.08
                                end
                        when (
                            REGEXP_CONTAINS(seller_sku, r'^R_KNEE-SLE*') -- Knee Sleeves
                            or REGEXP_CONTAINS(seller_sku, r'^R_ELBO-SLE*') -- Elbow Sleeves
                        )
                        and (
                            marketplace = 'US'
                            or marketplace = 'UK'
                        )
                            then
                                case
                                    when REGEXP_CONTAINS(seller_sku, r'^*_2PC_*')
                                        then 0.15
                                    else 0.08
                                end
                        when
                            REGEXP_CONTAINS(seller_sku, r'^R_HIKE-SOC*') -- Merino Wool Hiking Socks
                            and marketplace = 'UK'
                            then
                                case
                                    when REGEXP_CONTAINS(seller_sku, r'^*_3PR_*')
                                        then 0.15
                                    else 0.08
                                end
                    end
        end as referral_fee_pct
        -- SUM(item_price_amount) * 0.15 as referral_fees, -- old formula for Bare Barrel only

    from joined_orders_with_added_fields

    group by purchase_date, marketplace, asin, seller_sku, item_price_currency_code, is_vine, is_replacement_order, tenant_id, order_status

    order by purchase_date desc, marketplace desc, quantity_ordered desc
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
        referral_fee_pct,
        referral_fee_pct * (net_item_price_amount + shipping_price_amount - shipping_discount_amount + buyer_info_gift_wrap_price_amount)
            as referral_fees
    from aggregate_joined_orders_with_added_fields
)

select * from add_referral_fees;

