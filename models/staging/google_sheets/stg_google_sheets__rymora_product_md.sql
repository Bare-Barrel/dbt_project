-- stg_google_sheets__rymora_product_md.sql

with

rymora_product_md as (

    select * from {{ source('google_sheets','rymora_product_master_data') }}

),

rename_fields as (

    select
        string_field_0 as product_code,
        string_field_1 as product_group,
        string_field_2 as ean,
        string_field_3 as asin,
        string_field_4 as color,
        string_field_5 as product_size,
        string_field_6 as current_sku,
        string_field_7 as previous_sku,
        string_field_8 as old_sku,
        string_field_9 as old_sku_2,
        string_field_10 as moq,
        string_field_11 as lead_time_days,
        string_field_12 as pcs_per_carton,
        string_field_13 as _2020_rates_usd,
        string_field_14 as length_cm,
        string_field_15 as width_cm,
        string_field_16 as height_cm,
        string_field_17 as cbm,
        string_field_18 as n_weight_kg,
        string_field_19 as g_weight_kg,
        string_field_20 as length_inch,
        string_field_21 as width_inch,
        string_field_22 as height_inch,
        string_field_23 as cft,
        string_field_24 as n_weight_lb,
        string_field_25 as g_weight_lb,
        string_field_26 as per_unit_g,
        string_field_27 as per_unit_oz

    from rymora_product_md

),

remove_product_group_rows as (

    select *
    from rename_fields
    where
        asin is not null
        and asin is distinct from "ASIN"

),

remove_dollar_sign as (

    select
        product_code,
        product_group,
        ean,
        asin,
        color,
        product_size,
        current_sku,
        previous_sku,
        old_sku,
        old_sku_2,
        moq,
        lead_time_days,
        pcs_per_carton,
        case
            when _2020_rates_usd is null
                then _2020_rates_usd
            else REGEXP_EXTRACT(_2020_rates_usd, r"\d+\.\d*")
        end as _2020_rates_usd,
        length_cm,
        width_cm,
        height_cm,
        cbm,
        n_weight_kg,
        g_weight_kg,
        length_inch,
        width_inch,
        height_inch,
        cft,
        n_weight_lb,
        g_weight_lb,
        per_unit_g,
        per_unit_oz

    from remove_product_group_rows

),

cast_data_types as (

    select
        -- strings
        product_code,
        product_group,
        ean,
        asin,
        color,
        product_size,
        current_sku,
        previous_sku,
        old_sku,
        old_sku_2,

        -- numerics
        CAST(moq as integer) as moq,
        CAST(lead_time_days as integer) as lead_time_days,
        CAST(pcs_per_carton as integer) as pcs_per_carton,
        CAST(_2020_rates_usd as numeric) as _2020_rates_usd,
        CAST(length_cm as integer) as length_cm,
        CAST(width_cm as integer) as width_cm,
        CAST(height_cm as integer) as height_cm,
        CAST(cbm as numeric) as cbm,
        CAST(n_weight_kg as numeric) as n_weight_kg,
        CAST(g_weight_kg as numeric) as g_weight_kg,
        CAST(length_inch as numeric) as length_inch,
        CAST(width_inch as numeric) as width_inch,
        CAST(height_inch as numeric) as height_inch,
        CAST(cft as numeric) as cft,
        CAST(n_weight_lb as numeric) as n_weight_lb,
        CAST(g_weight_lb as numeric) as g_weight_lb,
        CAST(per_unit_g as numeric) as per_unit_g,
        CAST(per_unit_oz as numeric) as per_unit_oz

    from remove_dollar_sign

)

select * from cast_data_types
