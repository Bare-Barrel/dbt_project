-- int_aggregate_financial_events_item_fees.sql 03

{#
Possible fee types contained in item_fee__fee_amount_usd:
1	null
2	FixedClosingFee
3	DigitalServicesFee - active
4	ShippingHB - active
5	ShippingChargeback - active
6	RenewedProgramFee
7	GiftwrapCommission
8	FBAWeightBasedFee
9	SalesTaxCollectionFee - active
10	FBAPerUnitFulfillmentFee - active
11	GiftwrapChargeback - active
12	FBAPerOrderFulfillmentFee
13	PanAmericasChargebackFee
14	VariableClosingFee
15	Commission - active
16	DigitalServicesFeeFBA - active
#}

{{ config(materialized='view') }}

with

financial_events_item_fees_usd as (

    select * from {{ ref('int_convert_financial_events_item_fees_amount_to_usd') }}

),

{# deduped_fin_events_item_fees as (

    select * from {{ ref('int_dedup_financial_events_item_fees') }}

), #}

aggregate_financial_events_item_fees as (

    select
        amazon_order_id,
        order_item_id,
        seller_sku,
        marketplace,
        tenant_id,
        {# item_fee__currency_code, #}

        SUM(item_fee__fee_amount_usd) as item_fee__fee_amount_usd
    {# SUM(item_fee__fee_amount) as item_fee__fee_amount #}

    from financial_events_item_fees_usd

    group by    -- sum across posted_date
        amazon_order_id,
        order_item_id,
        seller_sku,
        marketplace,
        tenant_id
    {# item_fee__currency_code #}

)

select * from aggregate_financial_events_item_fees
