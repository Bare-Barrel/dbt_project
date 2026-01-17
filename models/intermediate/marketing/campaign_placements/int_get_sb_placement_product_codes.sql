-- int_get_sb_placement_product_codes.sql sb_cp_04

{{ config(materialized='view') }}

with

sb_placements_with_added_fields as (

    select * from {{ ref('int_calculate_fields_for_sb_placement') }}

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

bb_product_codes as (

    select
        parent_code,
        portfolio_code,
        product_code
    from {{ ref('dim_products') }}
    where tenant_id = 1

),

unique_bb_product_codes as (

    select distinct *
    from bb_product_codes

),

get_sb_product_codes as (

    select
        sb_cp_af.date,
        sb_cp_af.created_at,
        sb_cp_af.updated_at,
        sb_cp_af.campaign_id,
        sb_cp_af.campaign_name,
        sb_cp_af.campaign_status,
        sb_cp_af.portfolio_id,
        sb_cp_af.portfolio_name,
        sb_cp_af.marketplace,
        sb_cp_af.placement_classification,
        sb_cp_af.sb_ad_type,
        sb_cp_af.impressions,
        sb_cp_af.clicks,
        sb_cp_af.units_sold_clicks,
        sb_cp_af.new_to_brand_units_sold_clicks,
        sb_cp_af.purchases_clicks,
        sb_cp_af.tenant_id,
        sb_cp_af.campaign_budget_amount_usd,
        sb_cp_af.cost_usd,
        sb_cp_af.sales_clicks_usd,
        sb_cp_af.new_to_brand_sales_clicks_usd,
        sb_cp_af.cost_per_click_usd,
        sb_cp_af.click_through_rate,
        sb_cp_af.conversion_rate,

        -- Parent Code
        case
            when sb_cp_af.tenant_id = 1
                then u_bb_pc.parent_code
            when sb_cp_af.tenant_id = 2
                then ry_pc.parent_code
        end as parent_code,

        -- Portfolio Code
        case
            when sb_cp_af.tenant_id = 1
                then COALESCE(u_bb_pc.portfolio_code, sb_cp_af.portfolio_name)
        end as portfolio_code,

        -- Product Code
        case
            when sb_cp_af.tenant_id = 1
                then sb_cp_af.product_code
            when sb_cp_af.tenant_id = 2
                then ry_pc.product_code
        end as product_code,

        -- Product Color
        case
            when sb_cp_af.tenant_id = 2
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(0)])
        end as product_color,

        -- Product Pack Size
        case
            when sb_cp_af.tenant_id = 2
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(1)])
        end as product_pack_size

    from sb_placements_with_added_fields as sb_cp_af

    left join unique_bb_product_codes as u_bb_pc
        on sb_cp_af.product_code = u_bb_pc.product_code

    left join rymora_product_codes as ry_pc
        on sb_cp_af.asin = ry_pc.asin

),

standardize_product_color as (

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
        placement_classification,
        sb_ad_type,
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
        parent_code,
        portfolio_code,
        product_code,
        product_pack_size,

        -- Product Color
        case
            when tenant_id = 2
                then
                    case
                        when product_color = "BLK"
                            then "BLACK"
                        when product_color = "GRY"
                            then "GREY"
                        when product_color = "NA-GR"
                            then "NAVY/GREY"
                        else product_color
                    end
        end as product_color

    from get_sb_product_codes

),

get_sb_parent_codes as (

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
        placement_classification,
        sb_ad_type,
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
        portfolio_code,
        product_code,
        product_pack_size,
        product_color,

        -- Parent Code
        case
            when tenant_id = 1
                then COALESCE(parent_code, TRIM(SPLIT(portfolio_code, "-")[SAFE_OFFSET(0)]))
            when tenant_id = 2
                then parent_code
        end as parent_code

    from standardize_product_color

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
        placement_classification,
        sb_ad_type,
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

        COALESCE(parent_code, "UNKNOWN") as parent_code,
        COALESCE(portfolio_code, "UNKNOWN") as portfolio_code,
        COALESCE(product_code, "UNKNOWN") as product_code,
        COALESCE(product_color, "UNKNOWN") as product_color,
        COALESCE(product_pack_size, "UNKNOWN") as product_pack_size

    from get_sb_parent_codes

)

select * from fill_in_product_code_nulls
