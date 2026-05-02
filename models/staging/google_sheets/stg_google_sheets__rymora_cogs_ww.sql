-- stg_google_sheets__rymora_cogs_ww.sql

with

rymora_cogs_ww as (

    select * from {{ source('google_sheets','rymora_cogs_ww') }}

),

rename_and_filter_fields as (

    select
        string_field_0 as start_date,
        string_field_1 as end_date,
        string_field_2 as parent_product,
        string_field_3 as product_code,
        string_field_4 as sku,
        string_field_5 as ean,
        string_field_6 as asin,
        string_field_7 as product_pack_size,
        string_field_8 as product_color,
        string_field_9 as product_size, -- skip string_field_10
        string_field_11 as us_pieces_per_carton,
        string_field_12 as us_cogs_factory_fee_usd,
        string_field_13 as us_cogs_ddp_freight_usd,
        string_field_14 as us_cogs_3pl_usd,
        string_field_15 as us_cogs_usd,
        string_field_16 as us_fba_fee_usd,
        string_field_17 as us_storage_fee_usd,
        string_field_18 as us_returns_usd,
        string_field_19 as us_referral_rate,
        string_field_20 as us_avg_revenue_per_sale,
        string_field_21 as us_return_rate,  -- skip string_field_22
        string_field_23 as ca_fulfillment_type,
        string_field_24 as ca_pieces_per_carton,
        string_field_25 as ca_local_cogs_factory_fee_usd,
        string_field_26 as ca_local_cogs_ddp_freight_usd,
        string_field_27 as ca_local_cogs_3pl_usd,
        string_field_28 as ca_local_cogs_usd,
        string_field_29 as ca_local_cogs_cad,
        string_field_30 as ca_local_fba_fee_cad,
        string_field_31 as ca_local_storage_fee_cad,
        string_field_32 as ca_local_returns_cad,
        string_field_33 as ca_local_referral_rate,
        string_field_34 as ca_local_avg_revenue_per_sale,
        string_field_35 as ca_local_return_rate,
        string_field_36 as ca_narf_cogs_cad,
        string_field_37 as ca_narf_fba_fee_cad,
        string_field_38 as ca_narf_storage_fee_cad,
        string_field_39 as ca_narf_returns_cad,
        string_field_40 as ca_narf_referral_rate,
        string_field_41 as ca_narf_avg_revenue_per_sale,
        string_field_42 as ca_narf_return_rate, -- skip string_field_43
        string_field_44 as uk_pieces_per_carton,
        string_field_45 as uk_cogs_factory_fee_usd,
        string_field_46 as uk_cogs_ddp_freight_usd,
        string_field_47 as uk_cogs_3pl_usd,
        string_field_48 as uk_cogs_usd,
        string_field_49 as uk_cogs_gbp,
        string_field_50 as uk_fba_fee_gbp,
        string_field_51 as uk_storage_fee_gbp,
        string_field_52 as uk_returns_gbp,
        string_field_53 as uk_referral_rate,
        string_field_54 as uk_avg_revenue_per_sale,
        string_field_55 as uk_return_rate

    from rymora_cogs_ww

),

remove_header as (

    select *

    from rename_and_filter_fields

    where sku is distinct from "SKU"

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

    from remove_header

)

select * from cast_data_types
