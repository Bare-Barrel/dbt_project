-- int_aggregate_financial_events_item_fees.sql 03

{{ config(materialized='view') }}

with

{# financial_events_item_fees_usd as (

    select * from {{ ref('int_convert_financial_events_item_fees_amount_to_usd') }}

), #}

deduped_fin_events_item_fees as (

    select * from {{ ref('int_dedup_financial_events_item_fees') }}

),

aggregate_financial_events_item_fees as (

    select
        amazon_order_id,
        order_item_id,
        seller_sku,
        marketplace,
        tenant_id,
        item_fee__currency_code,

        {# SUM(item_fee__fee_amount_usd) as item_fee__fee_amount_usd #}
        SUM(item_fee__fee_amount) as item_fee__fee_amount

    from deduped_fin_events_item_fees

    group by    -- sum across posted_date
        amazon_order_id,
        order_item_id,
        seller_sku,
        marketplace,
        tenant_id,
        item_fee__currency_code

)

select * from aggregate_financial_events_item_fees
