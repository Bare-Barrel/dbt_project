-- int_pivot_bb_cogs_ww.sql 01

{{ config(materialized='ephemeral') }}

with

bb_cogs_ww as (

    select * from {{ ref('stg_google_sheets__bb_cogs_ww') }}

),

bb_us_cogs_ww as (

    select
        start_date,
        end_date,
        parent_product,
        product_code,
        sku,
        asin,

        1 as tenant_id,
        "US" as marketplace,
        "USD" as currency_code,

        us_cogs_usd as cogs,
        us_fba_fee_usd as fba_fee,
        us_storage_fee_usd as storage_fee,
        us_returns_usd as returns_cost,
        us_all_costs_except_commission_usd as all_costs_except_commission,
        us_avg_revenue_per_sale_usd as avg_revenue_per_sale,
        us_return_rate as return_rate

    from bb_cogs_ww

),

bb_ca_cogs_ww as (

    select
        start_date,
        end_date,
        parent_product,
        product_code,
        sku,
        asin,

        1 as tenant_id,
        "CA" as marketplace,
        "CAD" as currency_code,

        case
            when ca_fulfillment_type = "LOCAL"
                then ca_local_cogs_cad
            when ca_fulfillment_type = "NARF"
                then ca_narf_cogs_cad
        end as cogs,
        case
            when ca_fulfillment_type = "LOCAL"
                then ca_local_fba_fee_cad
            when ca_fulfillment_type = "NARF"
                then ca_narf_fba_fee_cad
        end as fba_fee,
        case
            when ca_fulfillment_type = "LOCAL"
                then ca_local_storage_fee_cad
            when ca_fulfillment_type = "NARF"
                then ca_narf_storage_fee_cad
        end as storage_fee,
        case
            when ca_fulfillment_type = "LOCAL"
                then ca_local_returns_cad
            when ca_fulfillment_type = "NARF"
                then ca_narf_returns_cad
        end as returns_cost,
        case
            when ca_fulfillment_type = "LOCAL"
                then ca_local_all_costs_except_commission_cad
            when ca_fulfillment_type = "NARF"
                then ca_narf_all_costs_except_commission_cad
        end as all_costs_except_commission,
        case
            when ca_fulfillment_type = "LOCAL"
                then ca_local_avg_revenue_per_sale_cad
            when ca_fulfillment_type = "NARF"
                then ca_narf_avg_revenue_per_sale_cad
        end as avg_revenue_per_sale,
        case
            when ca_fulfillment_type = "LOCAL"
                then ca_local_return_rate
            when ca_fulfillment_type = "NARF"
                then ca_narf_return_rate
        end as return_rate

    from bb_cogs_ww

),

bb_uk_cogs_ww as (

    select
        start_date,
        end_date,
        parent_product,
        product_code,
        sku,
        asin,

        1 as tenant_id,
        "UK" as marketplace,
        "GBP" as currency_code,

        uk_cogs_gbp as cogs,
        uk_fba_fee_gbp as fba_fee,
        uk_storage_fee_gbp as storage_fee,
        uk_returns_gbp as returns_cost,
        uk_all_costs_except_commission_gbp as all_costs_except_commission,
        uk_avg_revenue_per_sale_gbp as avg_revenue_per_sale,
        uk_return_rate as return_rate

    from bb_cogs_ww

),

union_all as (

    select * from bb_us_cogs_ww

    union all

    select * from bb_ca_cogs_ww

    union all

    select * from bb_uk_cogs_ww

)

select * from union_all
