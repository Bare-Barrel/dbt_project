-- stg_snapshots__bb_cogs_ww_snapshot.sql

with

bb_cogs_ww as (

    select * from {{ source('snapshots', 'bb_cogs_ww_snapshot') }}

),

rename_and_filter_fields as (

    select
        col_0 as start_date,
        col_1 as end_date,
        col_2 as parent_product,
        col_3 as product_code,
        col_4 as sku,
        col_5 as asin,
        col_6 as us_cogs_usd,
        col_7 as us_fba_fee_usd,
        col_8 as us_storage_fee_usd,
        col_9 as us_returns_usd,
        col_10 as us_all_costs_except_commission_usd,
        col_11 as us_avg_revenue_per_sale_usd,
        col_12 as us_return_rate, -- skip col_13
        col_14 as ca_fulfillment_type,
        col_15 as ca_local_cogs_cad,
        col_16 as ca_local_fba_fee_cad,
        col_17 as ca_local_storage_fee_cad,
        col_18 as ca_local_returns_cad,
        col_19 as ca_local_all_costs_except_commission_cad,
        col_20 as ca_local_avg_revenue_per_sale_cad,
        col_21 as ca_local_return_rate,
        col_22 as ca_narf_cogs_cad,
        col_23 as ca_narf_fba_fee_cad,
        col_24 as ca_narf_storage_fee_cad,
        col_25 as ca_narf_returns_cad,
        col_26 as ca_narf_all_costs_except_commission_cad,
        col_27 as ca_narf_avg_revenue_per_sale_cad,
        col_28 as ca_narf_return_rate, -- skip col_29
        col_30 as uk_cogs_gbp,
        col_31 as uk_fba_fee_gbp,
        col_32 as uk_storage_fee_gbp,
        col_33 as uk_returns_gbp,
        col_34 as uk_all_costs_except_commission_gbp,
        col_35 as uk_avg_revenue_per_sale_gbp,
        col_36 as uk_return_rate

    from bb_cogs_ww

),

remove_rows_with_blank_dates as (

    select *

    from rename_and_filter_fields

    where TRIM(start_date) != "" and TRIM(end_date) != ""

),

cast_data_types as (

    select
        -- dates
        CAST(start_date as date) as start_date,
        CAST(end_date as date) as end_date,

        -- strings
        parent_product,
        product_code,
        sku,
        asin,
        ca_fulfillment_type,

        -- numerics
        -- US
        SAFE_CAST(REGEXP_REPLACE(us_cogs_usd, r"[^0-9.-]", "") as numeric) as us_cogs_usd,
        SAFE_CAST(REGEXP_REPLACE(us_fba_fee_usd, r"[^0-9.-]", "") as numeric) as us_fba_fee_usd,
        SAFE_CAST(REGEXP_REPLACE(us_storage_fee_usd, r"[^0-9.-]", "") as numeric) as us_storage_fee_usd,
        SAFE_CAST(REGEXP_REPLACE(us_returns_usd, r"[^0-9.-]", "") as numeric) as us_returns_usd,
        SAFE_CAST(REGEXP_REPLACE(us_all_costs_except_commission_usd, r"[^0-9.-]", "") as numeric) as us_all_costs_except_commission_usd,
        SAFE_CAST(REGEXP_REPLACE(us_avg_revenue_per_sale_usd, r"[^0-9.-]", "") as numeric) as us_avg_revenue_per_sale_usd,
        SAFE_CAST(REPLACE(us_return_rate, "%", "") as float64) / 100 as us_return_rate,
        -- CA
        SAFE_CAST(REGEXP_REPLACE(ca_local_cogs_cad, r"[^0-9.-]", "") as numeric) as ca_local_cogs_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_fba_fee_cad, r"[^0-9.-]", "") as numeric) as ca_local_fba_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_storage_fee_cad, r"[^0-9.-]", "") as numeric) as ca_local_storage_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_returns_cad, r"[^0-9.-]", "") as numeric) as ca_local_returns_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_all_costs_except_commission_cad, r"[^0-9.-]", "") as numeric) as ca_local_all_costs_except_commission_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_avg_revenue_per_sale_cad, r"[^0-9.-]", "") as numeric) as ca_local_avg_revenue_per_sale_cad,
        SAFE_CAST(REPLACE(ca_local_return_rate, "%", "") as float64) / 100 as ca_local_return_rate,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_cogs_cad, r"[^0-9.-]", "") as numeric) as ca_narf_cogs_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_fba_fee_cad, r"[^0-9.-]", "") as numeric) as ca_narf_fba_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_storage_fee_cad, r"[^0-9.-]", "") as numeric) as ca_narf_storage_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_returns_cad, r"[^0-9.-]", "") as numeric) as ca_narf_returns_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_all_costs_except_commission_cad, r"[^0-9.-]", "") as numeric) as ca_narf_all_costs_except_commission_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_avg_revenue_per_sale_cad, r"[^0-9.-]", "") as numeric) as ca_narf_avg_revenue_per_sale_cad,
        SAFE_CAST(REPLACE(ca_narf_return_rate, "%", "") as float64) / 100 as ca_narf_return_rate,
        -- UK
        SAFE_CAST(REGEXP_REPLACE(uk_cogs_gbp, r"[^0-9.-]", "") as numeric) as uk_cogs_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_fba_fee_gbp, r"[^0-9.-]", "") as numeric) as uk_fba_fee_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_storage_fee_gbp, r"[^0-9.-]", "") as numeric) as uk_storage_fee_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_returns_gbp, r"[^0-9.-]", "") as numeric) as uk_returns_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_all_costs_except_commission_gbp, r"[^0-9.-]", "") as numeric) as uk_all_costs_except_commission_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_avg_revenue_per_sale_gbp, r"[^0-9.-]", "") as numeric) as uk_avg_revenue_per_sale_gbp,
        SAFE_CAST(REPLACE(uk_return_rate, "%", "") as float64) / 100 as uk_return_rate

    from remove_rows_with_blank_dates

)

select * from cast_data_types
