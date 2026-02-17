-- int_get_actual_amazon_fees_of_joined_orders.sql 09

{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    on_schema_change='sync_all_columns'
) }}

with

joined_orders_usd as (

    select * from {{ ref('int_convert_order_amounts_to_usd') }}

    {% if is_incremental() %}
        where purchase_date >= date_sub(
            (select max(t.purchase_date) from {{ this }} as t),
            interval 2 day
        )
    {% endif %}

),

aggregated_financial_events_item_fees as (

    select * from {{ ref('int_aggregate_financial_events_item_fees') }}

),

get_actual_amazon_fees as (

    select
        j_o_usd.amazon_order_id,
        j_o_usd.order_item_id,
        j_o_usd.purchase_datetime,
        j_o_usd.purchase_date,
        j_o_usd.marketplace,
        j_o_usd.sales_channel,
        j_o_usd.asin,
        j_o_usd.seller_sku,
        j_o_usd.order_status,
        j_o_usd.quantity_ordered,
        j_o_usd.promotion_ids,
        j_o_usd.product_info_number_of_items,
        j_o_usd.tax_collection_model,
        j_o_usd.tax_collection_responsible_party,
        j_o_usd.is_prime,
        j_o_usd.is_replacement_order,
        j_o_usd.replaced_order_id,
        j_o_usd.is_gift,
        j_o_usd.is_vine,
        j_o_usd.tenant_id,
        j_o_usd.item_price_amount_usd,
        j_o_usd.net_item_price_amount_usd,
        j_o_usd.coupon_fee_usd,
        j_o_usd.item_tax_amount_usd,
        j_o_usd.uk_output_vat_usd,
        j_o_usd.promotion_discount_amount_usd,
        j_o_usd.promotion_discount_tax_amount_usd,
        j_o_usd.shipping_price_amount_usd,
        j_o_usd.shipping_discount_amount_usd,
        j_o_usd.buyer_info_gift_wrap_price_amount_usd,
        j_o_usd.cogs_usd,
        j_o_usd.est_fba_fee_usd,
        j_o_usd.est_storage_fee_usd,
        j_o_usd.est_returns_cost_usd,
        j_o_usd.est_referral_fee_usd,
        agg_feif.item_fee__fee_amount_usd as actual_amazon_fees_usd

    from joined_orders_usd as j_o_usd

    left join aggregated_financial_events_item_fees as agg_feif
        on
            j_o_usd.amazon_order_id = agg_feif.amazon_order_id
            and j_o_usd.order_item_id = agg_feif.order_item_id
            and j_o_usd.seller_sku = agg_feif.seller_sku
            and j_o_usd.marketplace = agg_feif.marketplace
            and j_o_usd.tenant_id = agg_feif.tenant_id

)

select * from get_actual_amazon_fees
