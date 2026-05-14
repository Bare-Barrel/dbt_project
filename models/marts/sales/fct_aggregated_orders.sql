-- fct_aggregated_orders.sql

with

agg_orders_with_calc_fields as (

    select * from {{ ref('int_calculate_fields_for_aggregated_joined_orders') }}

),

dim_marketplace as (

    select * from {{ ref('dim_marketplace') }}

),

dim_product as (

    select * from {{ ref('dim_product') }}

),

dim_tenant as (

    select * from {{ ref('dim_tenant') }}

),

add_surrogate_keys as (

    select
        a_o_cf.purchase_date,

        dpr.product_sk,
        dmp.marketplace_sk,
        dtn.tenant_sk,

        {# a_o_cf.marketplace, #}
        a_o_cf.order_status,
        {# a_o_cf.asin, #}
        {# a_o_cf.seller_sku, #}
        {# a_o_cf.product_code, #}
        a_o_cf.is_vine,
        a_o_cf.is_replacement_order,
        {# a_o_cf.tenant_id, #}
        a_o_cf.quantity_ordered,
        a_o_cf.item_price_amount_usd,
        a_o_cf.coupon_fee_usd,
        a_o_cf.item_tax_amount_usd,
        a_o_cf.uk_output_vat_usd,
        a_o_cf.promotion_discount_amount_usd,
        a_o_cf.net_item_price_per_unit_usd,
        a_o_cf.net_item_price_amount_usd,
        a_o_cf.cogs_usd,
        {# a_o_cf.est_fba_fee_usd, #}
        a_o_cf.est_storage_fee_usd,
        a_o_cf.est_returns_cost_usd,
        {# a_o_cf.est_referral_fee_usd, #}
        {# a_o_cf.est_amazon_fees_usd, #}
        {# a_o_cf.actual_amazon_fees_usd, #}
        a_o_cf.amazon_fees_usd

    from agg_orders_with_calc_fields as a_o_cf

    left join dim_product as dpr
        on a_o_cf.asin = dpr.asin

    left join dim_marketplace as dmp
        on a_o_cf.marketplace = dmp.marketplace_name

    left join dim_tenant as dtn
        on a_o_cf.tenant_id = dtn.tenant_id

    order by a_o_cf.purchase_date desc, a_o_cf.marketplace desc, a_o_cf.quantity_ordered desc

)

select * from add_surrogate_keys
