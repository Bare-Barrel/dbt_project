-- obt_orders_per_asin_and_date.sql
-- same as amazon_orders_summary_view

with

aggregated_joined_orders as (

    select * from {{ ref('int_aggregate_joined_orders_with_added_fields') }}

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
        tenant_id

    from aggregated_joined_orders

    order by purchase_date desc, marketplace desc, quantity_ordered desc
)

select * from reorder_fields
