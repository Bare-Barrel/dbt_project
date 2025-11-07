

  create or replace view `modern-sublime-383117`.`dbt_cherry_intermediate`.`int_add_fields_to_joined_orders`
  OPTIONS()
  as -- int_add_fields_to_joined_orders.sql
-- same as amazon_order_items_detailed_view



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
),  __dbt__cte__int_calculate_fields_for_joined_orders as (
-- int_calculate_fields_for_joined_orders.sql



with

joined_orders as (

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
),  __dbt__cte__int_get_prime_exclusive_price_for_new_orders as (
-- int_get_prime_exclusive_price_for_new_orders.sql



with

orders as (

    select * from `modern-sublime-383117`.`orders`.`amazon_orders`

),

order_items as (

    select * from `modern-sublime-383117`.`dbt_cherry_staging`.`stg_orders__amazon_order_items`

),

competitive_pricing as (

    select * from `modern-sublime-383117`.`product_pricing`.`competitive_pricing`

),

prime_prices as (
    select
        asin,
        marketplace,
        (ARRAY_AGG(
            JSON_EXTRACT_SCALAR(
                product_competitive_pricing_competitive_prices,
                '$[0].Price.ListingPrice.Amount'
            ) order by date desc limit 1
        ))[OFFSET(0)] as prime_price
    from competitive_pricing
    where
        customer_type = 'Business'
        and product_competitive_pricing_competitive_prices != '[]'
    group by asin, marketplace
),

get_prime_exclusive_price as (

    select
        o.amazon_order_id,
        oi.order_item_id,

        case
            when
                o.order_status in ('Pending')
                and oi.item_price_amount is null
                and o.is_replacement_order = false
                then
                    CAST(pp.prime_price as numeric) * oi.quantity_ordered
            else oi.item_price_amount
        end as item_price_amount

    from orders as o

    left join order_items as oi
        on o.amazon_order_id = oi.amazon_order_id

    left join prime_prices as pp
        on
            oi.asin = pp.asin
            and oi.marketplace = pp.marketplace

)

select * from get_prime_exclusive_price
), fields_to_add as (

    select * from __dbt__cte__int_calculate_fields_for_joined_orders

),

with_prime_exclusive_price as (

    select * from __dbt__cte__int_get_prime_exclusive_price_for_new_orders

),

joined_orders as (

    select * from __dbt__cte__int_join_orders_and_order_items

),

add_fields_to_joined_orders as (

    select
        jo.amazon_order_id,
        jo.order_item_id,
        fta.purchase_datetime,
        fta.purchase_date,
        jo.marketplace,
        jo.sales_channel,
        jo.asin,
        jo.seller_sku,
        jo.order_status,
        jo.quantity_ordered,
        jo.promotion_ids,
        jo.product_info_number_of_items,
        jo.item_price_currency_code,
        prime.item_price_amount,
        jo.item_tax_amount,
        jo.promotion_discount_tax_currency_code,
        fta.output_vat,
        jo.item_tax_currency_code,
        jo.promotion_discount_tax_amount,
        jo.promotion_discount_currency_code,
        jo.promotion_discount_amount,
        fta.coupon_fee,
        jo.tax_collection_model,
        jo.tax_collection_responsible_party,
        jo.is_prime,
        jo.is_replacement_order,
        jo.replaced_order_id,
        jo.is_gift,
        fta.is_vine,
        jo.tenant_id,
        jo.shipping_price_amount,
        jo.shipping_price_currency_code,
        jo.shipping_discount_amount,
        jo.shipping_discount_currency_code,
        jo.buyer_info_gift_wrap_price_amount,
        jo.buyer_info_gift_wrap_price_currency_code

    from joined_orders as jo

    left join fields_to_add as fta
        on jo.order_item_id = fta.order_item_id

    left join with_prime_exclusive_price as prime
        on jo.order_item_id = prime.order_item_id

    order by fta.purchase_datetime desc, jo.tenant_id asc, jo.marketplace desc, jo.seller_sku asc
)

select * from add_fields_to_joined_orders;

