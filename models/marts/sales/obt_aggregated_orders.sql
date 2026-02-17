-- obt_aggregated_orders.sql

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
        is_vine,
        is_replacement_order,
        tenant_id,
        quantity_ordered,
        item_price_amount_usd,
        coupon_fee_usd,
        item_tax_amount_usd,
        uk_output_vat_usd,
        promotion_discount_amount_usd,
        net_item_price_per_unit_usd,
        net_item_price_amount_usd,
        cogs_usd,
        {# est_fba_fee_usd, #}
        est_storage_fee_usd,
        est_returns_cost_usd,
        {# est_referral_fee_usd, #}
        {# est_amazon_fees_usd, #}
        {# actual_amazon_fees_usd, #}
        amazon_fees_usd

    from agg_orders_with_calc_fields

    order by purchase_date desc, marketplace desc, quantity_ordered desc

)

select * from reorder_fields
