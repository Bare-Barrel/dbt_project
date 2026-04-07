-- stg_product_fees__product_fees_estimates.sql

with

product_fees_estimates as (

    select * from {{ source('product_fees','product_fees_estimates') }}

),

cast_data_types as (

    select
        -- ids
        tenant_id,

        -- strings
        marketplace,
        sku,
        price_to_estimate_fees_currency_code,

        -- numerics
        price_to_estimate_fees_amount,
        est_total_fees,
        est_referral_fee,
        est_variable_closing_fee,
        est_per_item_fee,
        est_fba_fee,

        -- datetime
        TIMESTAMP(fees_estimated_at) as fees_estimated_at,
        recorded_at

    from product_fees_estimates

)

select * from cast_data_types
