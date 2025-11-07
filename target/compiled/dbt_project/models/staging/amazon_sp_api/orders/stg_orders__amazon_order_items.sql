-- stg_orders__amazon_order_items.sql

with

source as (

    select * from `modern-sublime-383117`.`orders`.`amazon_order_items`

),

handle_nulls as (

    select
        -- ids
        asin,
        tenant_id,
        order_item_id,
        promotion_ids,
        amazon_order_id,

        -- strings
        title,
        seller_sku,
        marketplace,
        tax_collection_model,
        item_tax_currency_code,
        item_price_currency_code,
        shipping_tax_currency_code,
        shipping_price_currency_code,
        shipping_discount_currency_code,
        promotion_discount_currency_code,
        tax_collection_responsible_party,
        shipping_discount_tax_currency_code,
        promotion_discount_tax_currency_code,
        buyer_info_gift_wrap_tax_currency_code,
        buyer_info_gift_wrap_price_currency_code,

        -- numerics
        quantity_ordered,
        quantity_shipped,
        shipping_tax_amount,
        shipping_price_amount,
        shipping_discount_amount,
        product_info_number_of_items,
        shipping_discount_tax_amount,
        buyer_info_gift_wrap_tax_amount,
        buyer_info_gift_wrap_price_amount,

        -- booleans
        is_gift,
        is_transparency,

        -- dates and timestamps
        created_at,
        updated_at,

        COALESCE(item_tax_amount, 0) as item_tax_amount,
        COALESCE(item_price_amount, 0) as item_price_amount,
        COALESCE(promotion_discount_amount, 0) as promotion_discount_amount,
        COALESCE(promotion_discount_tax_amount, 0) as promotion_discount_tax_amount

    from source

)

select * from handle_nulls