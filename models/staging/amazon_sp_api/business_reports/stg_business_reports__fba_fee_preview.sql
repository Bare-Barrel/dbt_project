-- stg_business_reports__fba_fee_preview.sql

with

fba_fee_preview as (

    select * from {{ source('business_reports','fba_fee_preview') }}

),

select_fields as (

    select
        sku,
        asin,
        date,
        brand,
        fnsku,
        currency,
        tenant_id,
        created_at,
        updated_at,
        your_price,
        median_side,
        sales_price,
        amazon_store,
        fulfilled_by,
        longest_side,
        product_name,
        product_group,
        shortest_side,
        unit_of_weight,
        length_and_girth,
        product_size_tier,
        unit_of_dimension,
        estimated_fee_total,
        item_package_weight,
        estimated_variable_closing_fee,
        estimated_referral_fee_per_unit,
        estimated_pick_pack_fee_per_unit,
        expected_fulfillment_fee_per_unit,
        estimated_order_handling_fee_per_order,
        estimated_weight_handling_fee_per_unit

    from fba_fee_preview

),

filter_max_date_rows as (

    select *

    from select_fields

    where date = (
        select MAX(ffp.date)
        from `business_reports.fba_fee_preview` as ffp
    )

)

select * from filter_max_date_rows
