-- int_compute_uk_vat.sql 20%

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
), order_items as (

    select * from __dbt__cte__int_join_orders_and_order_items

),

compute_uk_vat as (

    select
        t1.amazon_order_id,
        t2.order_item_id,
        case
            when t1.marketplace = 'UK' and t2.item_tax_amount > 0
                then t2.item_tax_amount
            when t1.marketplace = 'UK' and t2.item_tax_amount = 0
                then (t2.item_price_amount / 1.20) * 0.20
            else 0
        end as output_vat

    from orders as t1

    left join order_items as t2
        on t1.amazon_order_id = t2.amazon_order_id

)

select * from compute_uk_vat