-- int_aggregate_financial_events_item_fees.sql 03

{{ config(materialized='view') }}

with

financial_events_item_fees_usd as (

    select * from {{ ref('int_convert_financial_events_item_fees_amount_to_usd') }}

),

aggregate_financial_events_item_fees as (

    select
        amazon_order_id,
        order_item_id,
        seller_sku,
        marketplace,
        tenant_id,

        SUM(item_fee__fee_amount_usd) as item_fee__fee_amount_usd

    from financial_events_item_fees_usd

    group by    -- sum across posted_date
        amazon_order_id,
        order_item_id,
        seller_sku,
        marketplace,
        tenant_id

)

select * from aggregate_financial_events_item_fees
