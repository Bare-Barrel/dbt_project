-- int_add_fields_to_product_fees_estimates.sql

{{ config(materialized='view') }}

with

stg_product_fees_estimates as (

    select * from {{ ref('stg_product_fees__product_fees_estimates') }}

),

stg_fba_fee_preview as (

    select * from {{ ref('stg_business_reports__fba_fee_preview') }}

),

add_fields as (

    select
        pfe.tenant_id,
        pfe.marketplace,
        pfe.sku,
        pfe.price_to_estimate_fees_amount,
        pfe.price_to_estimate_fees_currency_code,
        pfe.fees_estimated_at,
        pfe.est_total_fees,
        pfe.est_referral_fee,
        pfe.est_variable_closing_fee,
        pfe.est_per_item_fee,
        pfe.est_fba_fee,
        pfe.error_code,
        pfe.error_message,
        pfe.recorded_at,

        SAFE_DIVIDE(pfe.est_referral_fee, pfe.price_to_estimate_fees_amount) as est_referral_fee_pct,
        DATE(pfe.recorded_at) as recorded_date,

        ffp.product_name,
        ffp.product_group,
        ffp.longest_side,
        ffp.shortest_side,
        ffp.median_side,
        ffp.length_and_girth,
        ffp.unit_of_dimension,
        ffp.item_package_weight,
        ffp.unit_of_weight,
        ffp.product_size_tier

    from stg_product_fees_estimates as pfe

    left join stg_fba_fee_preview as ffp
        on
            pfe.sku = ffp.sku
            and pfe.tenant_id = ffp.tenant_id

)

select * from add_fields
