-- stg_snapshots__rymora_product_md_snapshot.sql

with

rymora_product_md as (

    select * from {{ source('snapshots','rymora_product_master_data_snapshot') }}

),

rename_fields as (

    select
        col_0 as product_code,
        col_1 as product_group,
        col_2 as ean,
        col_3 as asin,
        col_4 as color,
        col_5 as product_size,
        col_6 as current_sku,
        col_7 as previous_sku,
        col_8 as old_sku,
        col_9 as old_sku_2,
        col_10 as moq,
        col_11 as lead_time_days,
        col_12 as pcs_per_carton,
        col_13 as _2020_rates_usd,
        col_14 as length_cm,
        col_15 as width_cm,
        col_16 as height_cm,
        col_17 as cbm,
        col_18 as n_weight_kg,
        col_19 as g_weight_kg,
        col_20 as length_inch,
        col_21 as width_inch,
        col_22 as height_inch,
        col_23 as cft,
        col_24 as n_weight_lb,
        col_25 as g_weight_lb,
        col_26 as per_unit_g,
        col_27 as per_unit_oz

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
        SAFE_CAST(moq as integer) as moq,
        SAFE_CAST(lead_time_days as integer) as lead_time_days,
        SAFE_CAST(pcs_per_carton as integer) as pcs_per_carton,
        SAFE_CAST(_2020_rates_usd as numeric) as _2020_rates_usd,
        SAFE_CAST(length_cm as integer) as length_cm,
        SAFE_CAST(width_cm as integer) as width_cm,
        SAFE_CAST(height_cm as integer) as height_cm,
        SAFE_CAST(cbm as numeric) as cbm,
        SAFE_CAST(n_weight_kg as numeric) as n_weight_kg,
        SAFE_CAST(g_weight_kg as numeric) as g_weight_kg,
        SAFE_CAST(length_inch as numeric) as length_inch,
        SAFE_CAST(width_inch as numeric) as width_inch,
        SAFE_CAST(height_inch as numeric) as height_inch,
        SAFE_CAST(cft as numeric) as cft,
        SAFE_CAST(n_weight_lb as numeric) as n_weight_lb,
        SAFE_CAST(g_weight_lb as numeric) as g_weight_lb,
        SAFE_CAST(per_unit_g as numeric) as per_unit_g,
        SAFE_CAST(per_unit_oz as numeric) as per_unit_oz

    from remove_dollar_sign

)

select * from cast_data_types
