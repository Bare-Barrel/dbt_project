-- int_get_sd_product_codes.sql sd_c_04

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
    from {{ ref('dim_products') }}
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
        sd_c_af.asin,
        ry_pc.product_code,

        -- Parent Code
        case
            when sd_c_af.tenant_id = 1
                then ry_pc.parent_code
            when sd_c_af.tenant_id = 2
                then
                    case
                        when REGEXP_CONTAINS(sd_c_af.campaign_name, r"^All")
                            then "OTHER"
                        else ry_pc.parent_code
                    end
        end as parent_code,

        -- Product Color
        case
            when sd_c_af.tenant_id = 2
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(0)])
        end as product_color,

        -- Product Pack Size
        case
            when
                sd_c_af.tenant_id = 2
                and REGEXP_CONTAINS(TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(1)]), r"(PR|PC|V4)")
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(1)])
        end as product_pack_size

    from sd_campaigns_with_added_fields as sd_c_af

    left join rymora_product_codes as ry_pc
        on sd_c_af.asin = ry_pc.asin

),

standardize_product_color_and_pack as (

    select
        date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        marketplace,
        impressions,
        clicks,
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        asin,
        product_code,
        parent_code,

        -- Product Color
        case
            when tenant_id = 2
                then
                    case
                        when product_color = "BLK"
                            then "BLACK"
                        when product_color = "PIN"
                            then "PINK"
                        when
                            product_color = "GRY"
                            or product_color = "LET"
                            then "GREY"
                        when product_color = "WHT"
                            then "WHITE"
                        when product_color = "BLU"
                            then "BLUE"
                        when product_color = "FLU"
                            then "FLUORESCENT"
                        when product_color = "PUR"
                            then "PURPLE"
                        when product_color = "BK-BK"
                            then "BLACK/BLACK"
                        when product_color = "NA-GR"
                            then "NAVY/GREY"
                        when product_color = "PU-TE"
                            then "PURPLE/TEAL"
                        else product_color
                    end
        end as product_color,

        -- Product Pack Size
        case
            when
                tenant_id = 2
                and REGEXP_CONTAINS(product_pack_size, r"V4")
                then "1PR"
            else product_pack_size
        end as product_pack_size

    from get_sd_product_codes

),

fill_in_product_code_nulls as (

    select
        date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        marketplace,
        impressions,
        clicks,
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        asin,

        COALESCE(parent_code, "UNKNOWN") as parent_code,
        COALESCE(product_code, "UNKNOWN") as product_code,
        COALESCE(product_color, "UNKNOWN") as product_color,
        COALESCE(product_pack_size, "UNKNOWN") as product_pack_size

    from standardize_product_color_and_pack

)

select * from fill_in_product_code_nulls
