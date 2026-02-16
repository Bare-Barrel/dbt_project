-- int_convert_financial_events_item_fees_amount_to_usd.sql 02

{{ config(materialized='ephemeral') }}

with

deduped_fin_events_item_fees as (

    select * from {{ ref('int_dedup_financial_events_item_fees') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchangerate_host_api__exchange_rates') }}

),

join_fin_events_item_fees_and_fx_rates as (

    select
        fin_e_if.amazon_order_id,
        fin_e_if.order_item_id,
        fin_e_if.posted_date,
        fin_e_if.seller_sku,
        fin_e_if.marketplace,
        fin_e_if.tenant_id,
        fin_e_if.quantity_shipped,
        fin_e_if.item_fee__fee_type,
        fin_e_if.item_fee__currency_code,
        fin_e_if.item_fee__fee_amount,

        case
            when fin_e_if.item_fee__currency_code = "USD"
                then 1
            else fx.rate
        end as fx_rate

    from deduped_fin_events_item_fees as fin_e_if

    left join exchange_rates as fx
        on
            fin_e_if.item_fee__currency_code = fx.target
            and DATE(fin_e_if.posted_date) = fx.recorded_at

),

convert_amounts_to_usd as (

    select
        amazon_order_id,
        order_item_id,
        posted_date,
        seller_sku,
        marketplace,
        tenant_id,
        quantity_shipped,
        item_fee__fee_type,

        SAFE_DIVIDE(item_fee__fee_amount, fx_rate) as item_fee__fee_amount_usd

    from join_fin_events_item_fees_and_fx_rates

)

select * from convert_amounts_to_usd
