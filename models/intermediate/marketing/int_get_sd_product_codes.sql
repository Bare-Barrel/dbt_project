-- int_get_sd_product_codes.sql sd_04

{{ config(materialized='view') }}

with

sd_campaigns_with_added_fields as (

    select * from {{ ref('int_calculate_fields_for_sd_campaigns') }}

),

rymora_product_codes as (

    select
        asin,
        parent_code,
        shaker_code,
        product_code
    from {{ ref('dim_product_codes') }}
    where tenant_id = 2

),

get_sd_product_codes as (

    select
        sd_c_af.date,
        sd_c_af.created_at,
        sd_c_af.updated_at,
        sd_c_af.campaign_id,
        sd_c_af.campaign_name,
        sd_c_af.campaign_status,
        sd_c_af.portfolio_id,
        sd_c_af.portfolio_name,
        sd_c_af.marketplace,
        sd_c_af.impressions,
        sd_c_af.clicks,
        sd_c_af.units_sold_clicks,
        sd_c_af.new_to_brand_units_sold_clicks,
        sd_c_af.purchases_clicks,
        sd_c_af.tenant_id,
        sd_c_af.campaign_budget_amount_usd,
        sd_c_af.cost_usd,
        sd_c_af.sales_clicks_usd,
        sd_c_af.new_to_brand_sales_clicks_usd,
        sd_c_af.cost_per_click_usd,
        sd_c_af.click_through_rate,
        sd_c_af.conversion_rate,
        sd_c_af.product_group,
        sd_c_af.asin,
        ry_pc.parent_code,
        ry_pc.product_code,

        -- Product Color
        case
            when sd_c_af.tenant_id = 2
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(0)])
        end as product_color,

        -- Product Pack Size
        case
            when sd_c_af.tenant_id = 2
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(1)])
        end as product_pack_size

    from sd_campaigns_with_added_fields as sd_c_af

    left join rymora_product_codes as ry_pc
        on sd_c_af.asin = ry_pc.asin

)

select * from get_sd_product_codes
