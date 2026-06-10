-- obt_product_fees_estimates.sql

with

int_product_fees_estimates as (

    select * from {{ ref('int_add_fields_to_product_fees_estimates') }}

),

reorder_fields as (

    select
        tenant_id,
        marketplace,
        sku,
        product_name,
        product_group,
        longest_side,
        shortest_side,
        median_side,
        length_and_girth,
        unit_of_dimension,
        item_package_weight,
        unit_of_weight,
        product_size_tier,
        price_to_estimate_fees_amount,
        price_to_estimate_fees_currency_code,
        fees_estimated_at,
        est_total_fees,
        est_referral_fee,
        est_variable_closing_fee,
        est_per_item_fee,
        est_fba_fee,
        error_code,
        error_message,
        recorded_at,
        est_referral_fee_pct,
        recorded_date

    from int_product_fees_estimates

)

select * from reorder_fields
