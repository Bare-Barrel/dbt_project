-- int_calculate_fields_for_joined_orders.sql 03

{{ config(materialized='ephemeral') }}

with

joined_orders as (

    select * from {{ ref('int_join_orders_and_order_items') }}

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
        end as uk_output_vat,

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
