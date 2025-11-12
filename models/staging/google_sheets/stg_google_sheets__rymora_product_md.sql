-- stg_google_sheets__rymora_product_md.sql

with

rymora_product_md as (

    select * from {{ source('google_sheets','rymora_product_master_data') }}

),

remove_product_group_rows as (

    select *
    from rymora_product_md
    where
        `ASIN` is not null

),

rename_fields as (

    select
        `Product_Code` as product_code,
        `Product_Group` as product_group,
        `EAN` as ean,
        `ASIN` as asin,
        `Colour` as color,
        `Size` as product_size,
        `Current_SKU__NEW_` as current_sku,
        `Previous_SKU__OLD_` as previous_sku,
        `Previous_SKU__OLD_2_` as old_sku,
        `MOQ` as moq,
        `Lead_time__days_` as lead_time_days,
        `Pcs__CTN` as pcs_per_carton,
        _2020_rates as _2020_rates_usd,
        `L__cm_` as length_cm,
        `W__cm_` as width_cm,
        `H__cm_` as height_cm,
        `CBM` as cbm,
        `N_W__kg_` as n_weight_kg,
        `G_W__kg_` as g_weight_kg,
        `L__in_` as length_inch,
        `W__in_` as width_inch,
        `H__in_` as height_inch,
        `CFT` as cft,
        `N_W__lb_` as n_weight_lb,
        `G_W__lb_` as g_weight_lb,
        per_unit_g,
        per_unit_oz

    from remove_product_group_rows

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
        moq,
        lead_time_days,
        pcs_per_carton,
        case
            when _2020_rates_usd is null
                then _2020_rates_usd
            else REGEXP_EXTRACT(_2020_rates_usd, r'\d+\.\d*')
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

    from rename_fields

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
