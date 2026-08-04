-- stg_snapshots__rymora_cogs_ww_snapshot.sql

with

rymora_cogs_ww as (

    select * from {{ source('snapshots','rymora_cogs_ww_snapshot') }}

),

rename_and_filter_fields as (

    select
        col_0 as start_date,
        col_1 as end_date,
        col_2 as parent_product,
        col_3 as product_code,
        col_4 as sku,
        col_5 as ean,
        col_6 as asin,
        col_7 as product_pack_size,
        col_8 as product_color,
        col_9 as product_size, -- skip col_10
        col_11 as us_pieces_per_carton,
        col_12 as us_cogs_factory_fee_usd,
        col_13 as us_cogs_ddp_freight_usd,
        col_14 as us_cogs_3pl_usd,
        col_15 as us_cogs_usd,
        col_16 as us_fba_fee_usd,
        col_17 as us_storage_fee_usd,
        col_18 as us_returns_usd,
        col_19 as us_referral_rate,
        col_20 as us_avg_revenue_per_sale,
        col_21 as us_return_rate,  -- skip col_22
        col_23 as ca_fulfillment_type,
        col_24 as ca_pieces_per_carton,
        col_25 as ca_local_cogs_factory_fee_usd,
        col_26 as ca_local_cogs_ddp_freight_usd,
        col_27 as ca_local_cogs_3pl_usd,
        col_28 as ca_local_cogs_usd,
        col_29 as ca_local_cogs_cad,
        col_30 as ca_local_fba_fee_cad,
        col_31 as ca_local_storage_fee_cad,
        col_32 as ca_local_returns_cad,
        col_33 as ca_local_referral_rate,
        col_34 as ca_local_avg_revenue_per_sale,
        col_35 as ca_local_return_rate,
        col_36 as ca_narf_cogs_cad,
        col_37 as ca_narf_fba_fee_cad,
        col_38 as ca_narf_storage_fee_cad,
        col_39 as ca_narf_returns_cad,
        col_40 as ca_narf_referral_rate,
        col_41 as ca_narf_avg_revenue_per_sale,
        col_42 as ca_narf_return_rate, -- skip col_43
        col_44 as uk_pieces_per_carton,
        col_45 as uk_cogs_factory_fee_usd,
        col_46 as uk_cogs_ddp_freight_usd,
        col_47 as uk_cogs_3pl_usd,
        col_48 as uk_cogs_usd,
        col_49 as uk_cogs_gbp,
        col_50 as uk_fba_fee_gbp,
        col_51 as uk_storage_fee_gbp,
        col_52 as uk_returns_gbp,
        col_53 as uk_referral_rate,
        col_54 as uk_avg_revenue_per_sale,
        col_55 as uk_return_rate

    from rymora_cogs_ww

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
        ean,
        asin,
        product_pack_size,
        product_color,
        product_size,
        ca_fulfillment_type,

        -- numerics
        -- US
        SAFE_CAST(us_pieces_per_carton as integer) as us_pieces_per_carton,
        SAFE_CAST(REGEXP_REPLACE(us_cogs_factory_fee_usd, r"[^0-9.-]", "") as numeric) as us_cogs_factory_fee_usd,
        SAFE_CAST(REGEXP_REPLACE(us_cogs_ddp_freight_usd, r"[^0-9.-]", "") as numeric) as us_cogs_ddp_freight_usd,
        SAFE_CAST(REGEXP_REPLACE(us_cogs_3pl_usd, r"[^0-9.-]", "") as numeric) as us_cogs_3pl_usd,
        SAFE_CAST(REGEXP_REPLACE(us_cogs_usd, r"[^0-9.-]", "") as numeric) as us_cogs_usd,
        SAFE_CAST(REGEXP_REPLACE(us_fba_fee_usd, r"[^0-9.-]", "") as numeric) as us_fba_fee_usd,
        SAFE_CAST(REGEXP_REPLACE(us_storage_fee_usd, r"[^0-9.-]", "") as numeric) as us_storage_fee_usd,
        SAFE_CAST(REGEXP_REPLACE(us_returns_usd, r"[^0-9.-]", "") as numeric) as us_returns_usd,
        SAFE_CAST(REPLACE(us_referral_rate, "%", "") as float64) / 100 as us_referral_rate,
        us_avg_revenue_per_sale,
        us_return_rate,
        -- CA
        SAFE_CAST(ca_pieces_per_carton as integer) as ca_pieces_per_carton,
        SAFE_CAST(REGEXP_REPLACE(ca_local_cogs_factory_fee_usd, r"[^0-9.-]", "") as numeric) as ca_local_cogs_factory_fee_usd,
        SAFE_CAST(REGEXP_REPLACE(ca_local_cogs_ddp_freight_usd, r"[^0-9.-]", "") as numeric) as ca_local_cogs_ddp_freight_usd,
        SAFE_CAST(REGEXP_REPLACE(ca_local_cogs_3pl_usd, r"[^0-9.-]", "") as numeric) as ca_local_cogs_3pl_usd,
        SAFE_CAST(REGEXP_REPLACE(ca_local_cogs_usd, r"[^0-9.-]", "") as numeric) as ca_local_cogs_usd,
        SAFE_CAST(REGEXP_REPLACE(ca_local_cogs_cad, r"[^0-9.-]", "") as numeric) as ca_local_cogs_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_fba_fee_cad, r"[^0-9.-]", "") as numeric) as ca_local_fba_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_storage_fee_cad, r"[^0-9.-]", "") as numeric) as ca_local_storage_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_local_returns_cad, r"[^0-9.-]", "") as numeric) as ca_local_returns_cad,
        SAFE_CAST(REPLACE(ca_local_referral_rate, "%", "") as float64) / 100 as ca_local_referral_rate,
        ca_local_avg_revenue_per_sale,
        ca_local_return_rate,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_cogs_cad, r"[^0-9.-]", "") as numeric) as ca_narf_cogs_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_fba_fee_cad, r"[^0-9.-]", "") as numeric) as ca_narf_fba_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_storage_fee_cad, r"[^0-9.-]", "") as numeric) as ca_narf_storage_fee_cad,
        SAFE_CAST(REGEXP_REPLACE(ca_narf_returns_cad, r"[^0-9.-]", "") as numeric) as ca_narf_returns_cad,
        SAFE_CAST(REPLACE(ca_narf_referral_rate, "%", "") as float64) / 100 as ca_narf_referral_rate,
        ca_narf_avg_revenue_per_sale,
        ca_narf_return_rate,
        -- UK
        SAFE_CAST(uk_pieces_per_carton as integer) as uk_pieces_per_carton,
        SAFE_CAST(REGEXP_REPLACE(uk_cogs_factory_fee_usd, r"[^0-9.-]", "") as numeric) as uk_cogs_factory_fee_usd,
        SAFE_CAST(REGEXP_REPLACE(uk_cogs_ddp_freight_usd, r"[^0-9.-]", "") as numeric) as uk_cogs_ddp_freight_usd,
        SAFE_CAST(REGEXP_REPLACE(uk_cogs_3pl_usd, r"[^0-9.-]", "") as numeric) as uk_cogs_3pl_usd,
        SAFE_CAST(REGEXP_REPLACE(uk_cogs_usd, r"[^0-9.-]", "") as numeric) as uk_cogs_usd,
        SAFE_CAST(REGEXP_REPLACE(uk_cogs_gbp, r"[^0-9.-]", "") as numeric) as uk_cogs_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_fba_fee_gbp, r"[^0-9.-]", "") as numeric) as uk_fba_fee_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_storage_fee_gbp, r"[^0-9.-]", "") as numeric) as uk_storage_fee_gbp,
        SAFE_CAST(REGEXP_REPLACE(uk_returns_gbp, r"[^0-9.-]", "") as numeric) as uk_returns_gbp,
        SAFE_CAST(REPLACE(uk_referral_rate, "%", "") as float64) / 100 as uk_referral_rate,
        uk_avg_revenue_per_sale,
        uk_return_rate

    from rename_and_filter_fields

)

select * from cast_data_types
