-- int_calculate_fields_for_joined_orders.sql



with

 __dbt__cte__int_join_orders_and_order_items as (
-- int_join_orders_and_order_items.sql



with

orders as (

    select * from `modern-sublime-383117`.`orders`.`amazon_orders`

),

order_items as (

    select * from `modern-sublime-383117`.`dbt_cherry_staging`.`stg_orders__amazon_order_items`

),

join_orders_and_order_items as (
    select
        o.amazon_order_id,
        oi.order_item_id,
        o.marketplace,
        o.sales_channel,
        oi.asin,
        oi.seller_sku,
        o.order_status,
        oi.quantity_ordered,
        oi.promotion_ids,
        oi.product_info_number_of_items,
        o.is_replacement_order,
        oi.item_price_amount,
        oi.item_price_currency_code,
        oi.item_tax_amount,
        oi.item_tax_currency_code,
        oi.promotion_discount_tax_amount,
        oi.promotion_discount_tax_currency_code,
        oi.promotion_discount_amount,
        oi.promotion_discount_currency_code,
        oi.tax_collection_model,
        oi.tax_collection_responsible_party,
        o.is_prime,
        o.replaced_order_id,
        oi.is_gift,
        o.tenant_id,
        o.purchase_date,
        oi.shipping_price_amount,
        oi.shipping_price_currency_code,
        oi.shipping_discount_amount,
        oi.shipping_discount_currency_code,
        oi.buyer_info_gift_wrap_price_amount,
        oi.buyer_info_gift_wrap_price_currency_code

    from orders as o

    left join order_items as oi
        on o.amazon_order_id = oi.amazon_order_id

    order by o.purchase_date desc, o.tenant_id asc, o.marketplace desc, oi.seller_sku asc

)

select * from join_orders_and_order_items
), joined_orders as (

    select * from __dbt__cte__int_join_orders_and_order_items

),

calculate_fields as (

    select
        order_item_id,

        -- purchase_date
        purchase_date as purchase_datetime,
        DATE(purchase_date) as purchase_date,

        -- UK VAT of 20%
        case
            when marketplace = 'UK' and item_tax_amount > 0
                then item_tax_amount
            when marketplace = 'UK' and item_tax_amount = 0
                then (item_price_amount / 1.20) * 0.20
            else 0
        end as output_vat,

        -- Coupon Fee
        case
            when
                promotion_ids like '%VPC%'
                and marketplace in ('US', 'CA')
                then 0.6
            when
                promotion_ids like '%VPC%'
                and marketplace = 'UK'
                then 0.45
            when promotion_ids like '%PLM%'
                then item_price_amount * 0.025
            else 0
        end as coupon_fee,

        -- Determines if Vine order
        case
            when
                order_status not in ('Pending', 'Canceled')
                and item_price_amount is null
                and is_replacement_order = false
                and quantity_ordered > 0
                then true
            when
                promotion_ids like '%vine%'
                then true
            else false
        end as is_vine

    from joined_orders

)

select * from calculate_fields