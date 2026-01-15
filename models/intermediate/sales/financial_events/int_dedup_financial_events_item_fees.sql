-- int_dedup_financial_events_item_fees.sql 01

{{ config(materialized='view') }}

with

financial_events_item_fees as (

    select * from {{ ref('stg_finances__financial_events_item_fees') }}

),

deduplicate as (

    select
        amazon_order_id,
        order_item_id,
        posted_date,
        seller_sku,
        marketplace,
        tenant_id,
        created_at,
        quantity_shipped,
        item_fee__fee_type,
        item_fee__currency_code,
        item_fee__fee_amount

    from financial_events_item_fees

    qualify
        row_number() over (
            partition by
                amazon_order_id,
                order_item_id,
                posted_date,
                seller_sku,
                marketplace,
                tenant_id,
                item_fee__fee_type,
                item_fee__currency_code
            order by created_at desc
        ) = 1

)

select * from deduplicate
