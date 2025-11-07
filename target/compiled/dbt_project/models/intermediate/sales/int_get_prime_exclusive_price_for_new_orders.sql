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