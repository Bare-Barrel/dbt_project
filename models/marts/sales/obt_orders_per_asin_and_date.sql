-- obt_orders_per_asin_and_date.sql
-- same as amazon_orders_summary_view

with

agg_orders_with_calc_fields as (

    select * from {{ ref('int_calculate_fields_for_aggregated_joined_orders') }}

),

reorder_fields as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        product_code,
        quantity_ordered,
        net_item_price_per_unit,
        item_price_amount,
        promotion_discount_amount,
        item_tax_amount,
        output_vat,
        net_item_price_amount,
        referral_fees,
        coupon_fee,
        item_price_currency_code,
        is_vine,
        is_replacement_order,
        tenant_id,
        actual_amazon_fee_amount,
        actual_amazon_fee_currency_code

    from agg_orders_with_calc_fields

    order by purchase_date desc, marketplace desc, quantity_ordered desc
)

select * from reorder_fields
