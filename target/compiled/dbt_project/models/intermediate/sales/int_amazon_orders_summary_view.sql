-- int_aggregate_joined_orders_with_added_fields.sql

with

 __dbt__cte__int_join_orders_and_order_items as (
-- int_join_orders_and_order_items.sql

with

orders as (

    select * from `modern-sublime-383117`.`orders`.`amazon_orders`

),

order_items as (

    select * from `modern-sublime-383117`.`orders_orders`.`stg_orders__amazon_order_items`

),

join_orders_and_order_items as (
    select
        t1.amazon_order_id,
        t2.order_item_id,
        t1.marketplace,
        t1.sales_channel,
        t2.asin,
        t2.seller_sku,
        t1.order_status,
        t2.quantity_ordered,
        t2.promotion_ids,
        t2.product_info_number_of_items,
        t1.is_replacement_order,
        t2.item_price_amount,
        t2.item_price_currency_code,
        t2.item_tax_amount,
        t2.item_tax_currency_code,
        t2.promotion_discount_tax_amount,
        t2.promotion_discount_tax_currency_code,
        t2.promotion_discount_amount,
        t2.promotion_discount_currency_code,
        t2.tax_collection_model,
        t2.tax_collection_responsible_party,
        t1.is_prime,
        t1.replaced_order_id,
        t2.is_gift,
        t1.tenant_id,
        t1.purchase_date as purchase_datetime,
        DATE(t1.purchase_date) as purchase_date

    from orders as t1

    left join order_items as t2
        on t1.amazon_order_id = t2.amazon_order_id

    order by purchase_datetime desc, t1.tenant_id asc, t1.marketplace desc, t2.seller_sku asc

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

        -- UK VAT of 20%
        case
            when marketplace = 'UK' and item_tax_amount > 0
                then t2.item_tax_amount
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

    select * from `modern-sublime-383117`.`orders_orders`.`stg_orders__amazon_order_items`

),

competitive_pricing as (

    select * from `modern-sublime-383117`.`product_pricing`.`competitive_pricing`

),

t3 as (
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
        t1.amazon_order_id,
        t2.order_item_id,

        case
            when
                t1.order_status in ('Pending')
                and t2.item_price_amount is null
                and t1.is_replacement_order = false
                then
                    CAST(t3.prime_price as numeric) * t2.quantity_ordered
            else COALESCE(t2.item_price_amount, 0)
        end as item_price_amount

    from orders as t1

    left join order_items as t2
        on t1.amazon_order_id = t2.amazon_order_id

    left join t3
        on
            t2.asin = t3.asin
            and t2.marketplace = t3.marketplace

)

select * from get_prime_exclusive_price
),  __dbt__cte__int_add_fields_to_joined_orders as (
-- int_add_fields_to_joined_orders.sql

with

fields_to_add as (

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
        t1.amazon_order_id,
        t1.order_item_id,
        t1.purchase_datetime,
        t1.purchase_date,
        t1.marketplace,
        t1.sales_channel,
        t1.asin,
        t1.seller_sku,
        t1.order_status,
        t1.quantity_ordered,
        t1.promotion_ids,
        t1.product_info_number_of_items,
        t1.item_price_currency_code,
        t3.item_price_amount,
        t1.item_tax_amount,
        t1.promotion_discount_tax_currency_code,
        t2.output_vat,
        t1.item_tax_currency_code,
        t1.promotion_discount_tax_amount,
        t1.promotion_discount_currency_code,
        t1.promotion_discount_amount,
        t2.coupon_fee,
        t1.tax_collection_model,
        t1.tax_collection_responsible_party,
        t1.is_prime,
        t1.is_replacement_order,
        t1.replaced_order_id,
        t1.is_gift,
        t2.is_vine,
        t1.tenant_id

    from joined_orders as t1

    left join fields_to_add as t2
        on t1.order_item_id = t2.order_item_id

    left join with_prime_exclusive_price as t3
        on t1.order_item_id = t3.order_item_id

    order by t1.purchase_datetime desc, t1.tenant_id asc, t1.marketplace desc, t1.seller_sku asc
)

select * from add_fields_to_joined_orders
), joined_orders_with_added_fields as (

    select * from __dbt__cte__int_add_fields_to_joined_orders

),

aggregate_joined_orders_with_added_fields as (

    select
        purchase_date,
        marketplace,
        order_status,
        asin,
        seller_sku,
        REGEXP_EXTRACT(seller_sku, r'[^_]+$') AS product_code,
        SUM(quantity_ordered) quantity_ordered,
        -- UK Sales have 20% VAT included
        CASE
            WHEN marketplace = 'UK'
            THEN
                ROUND(
                    SAFE_DIVIDE(SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee) - SUM(output_vat), SUM(quantity_ordered)
                    ), 2)
            ELSE
                ROUND(
                    SAFE_DIVIDE(SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee), SUM(quantity_ordered)
                    ), 2)
            END AS net_item_price_per_unit,
        --
        SUM(item_price_amount) item_price_amount,
        SUM(promotion_discount_amount) promotion_discount_amount,
        SUM(item_tax_amount) item_tax_amount,
        SUM(output_vat) output_vat,
        -- UK Sales have 20% VAT included
        CASE
            WHEN marketplace = 'UK'
            THEN
                SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee) - SUM(output_vat)
            ELSE
                SUM(item_price_amount) - SUM(promotion_discount_amount) - SUM(coupon_fee)
        END AS net_item_price_amount,
        --
        -- Referral Fees of 15%
        SUM(item_price_amount) * 0.15 referral_fees,
        SUM(coupon_fee) coupon_fee,
        item_price_currency_code,
        is_vine,
        is_replacement_order,
        tenant_id
    from joined_orders_with_added_fields
    GROUP BY purchase_date, marketplace, asin, seller_sku, item_price_currency_code, is_vine, is_replacement_order, tenant_id, order_status
    ORDER BY purchase_date DESC, marketplace DESC, quantity_ordered DESC
)

select * from aggregate_joined_orders_with_added_fields