-- int_add_fields_to_product_fees_estimates.sql

{{ config(materialized='view') }}

with

stg_product_fees_estimates as (

    select * from {{ ref('stg_product_fees__product_fees_estimates') }}

),

add_fields as (

    select
        tenant_id,
        marketplace,
        sku,
        price_to_estimate_fees_amount,
        price_to_estimate_fees_currency_code,
        fees_estimated_at,
        est_total_fees,
        est_referral_fee,
        est_variable_closing_fee,
        est_per_item_fee,
        est_fba_fee,
        recorded_at,

        SAFE_DIVIDE(est_referral_fee, price_to_estimate_fees_amount) as est_referral_fee_pct,
        DATE(recorded_at) as recorded_date

    from stg_product_fees_estimates

)

select * from add_fields
