-- stg_finances__financial_events_item_fees.sql

with

source as (

    select * from {{ source('finances','financial_events') }}

),

explode_item_fee_list as (

    select
        fe.marketplace,
        fe.tenant_id,
        fe.created_at,

        fe.amazonorderid as amazon_order_id,
        fe.sellerorderid as seller_order_id,
        fe.marketplacename as marketplace_name,
        fe.posteddate as posted_date,
        fe.sellersku as seller_sku,
        fe.orderitemid as order_item_id,
        fe.quantityshipped as quantity_shipped,

        item_fee.feetype as item_fee__fee_type,
        item_fee.feeamount.currencycode as item_fee__currency_code,
        item_fee.feeamount.currencyamount as item_fee__fee_amount

    from source as fe

    left join unnest(fe.itemfeelist) as item_fee

)

select * from explode_item_fee_list
