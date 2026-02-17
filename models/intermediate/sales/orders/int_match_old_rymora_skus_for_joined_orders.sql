-- int_match_old_rymora_skus_for_joined_orders.sql 06

{{ config(materialized='view') }}

with

orders_with_cogs as (

    select * from {{ ref('int_get_cogs_fba_storage_returns_fees_for_joined_orders') }}

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
        o_w_c.*,

        -- new_sku to handle old Rymora skus
        case
            when REGEXP_CONTAINS(o_w_c.seller_sku, r'^RMA-SPO.*')
                then ry_p_md.current_sku
        end as new_sku

    from orders_with_cogs as o_w_c

    left join rymora_product_md as ry_p_md
        on o_w_c.seller_sku = ry_p_md.previous_sku

),

match_old_sku_2_to_current as (     -- match the second time around

    select
        m_o_sku.amazon_order_id,
        m_o_sku.order_item_id,
        m_o_sku.purchase_datetime,
        m_o_sku.purchase_date,
        m_o_sku.marketplace,
        m_o_sku.sales_channel,
        m_o_sku.asin,
        m_o_sku.seller_sku,
        m_o_sku.order_status,
        m_o_sku.quantity_ordered,
        m_o_sku.promotion_ids,
        m_o_sku.product_info_number_of_items,
        m_o_sku.item_price_currency_code,
        m_o_sku.item_price_amount,
        m_o_sku.item_tax_amount,
        m_o_sku.promotion_discount_tax_currency_code,
        m_o_sku.uk_output_vat,
        m_o_sku.item_tax_currency_code,
        m_o_sku.promotion_discount_tax_amount,
        m_o_sku.promotion_discount_currency_code,
        m_o_sku.promotion_discount_amount,
        m_o_sku.coupon_fee,
        m_o_sku.tax_collection_model,
        m_o_sku.tax_collection_responsible_party,
        m_o_sku.is_prime,
        m_o_sku.is_replacement_order,
        m_o_sku.replaced_order_id,
        m_o_sku.is_gift,
        m_o_sku.is_vine,
        m_o_sku.tenant_id,
        m_o_sku.shipping_price_amount,
        m_o_sku.shipping_price_currency_code,
        m_o_sku.shipping_discount_amount,
        m_o_sku.shipping_discount_currency_code,
        m_o_sku.buyer_info_gift_wrap_price_amount,
        m_o_sku.buyer_info_gift_wrap_price_currency_code,
        m_o_sku.est_fees_currency_code,
        m_o_sku.cogs,
        m_o_sku.est_fba_fee,
        m_o_sku.est_storage_fee,
        m_o_sku.est_returns_cost,

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

)

select * from match_old_sku_2_to_current
