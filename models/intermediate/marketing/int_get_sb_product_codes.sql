-- int_get_sb_product_codes.sql sb_04

{{ config(materialized='view') }}

with

sb_campaigns_with_added_fields as (

    select * from {{ ref('int_calculate_fields_for_sb_campaigns') }}

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
        sb_c_af.date,
        sb_c_af.created_at,
        sb_c_af.updated_at,
        sb_c_af.campaign_id,
        sb_c_af.campaign_name,
        sb_c_af.campaign_status,
        sb_c_af.portfolio_id,
        sb_c_af.portfolio_name,
        sb_c_af.marketplace,
        sb_c_af.impressions,
        sb_c_af.clicks,
        sb_c_af.units_sold_clicks,
        sb_c_af.new_to_brand_units_sold_clicks,
        sb_c_af.purchases_clicks,
        sb_c_af.top_of_search_impression_share,
        sb_c_af.tenant_id,
        sb_c_af.campaign_budget_amount_usd,
        sb_c_af.cost_usd,
        sb_c_af.sales_clicks_usd,
        sb_c_af.new_to_brand_sales_clicks_usd,
        sb_c_af.cost_per_click_usd,
        sb_c_af.click_through_rate,
        sb_c_af.conversion_rate,
        sb_c_af.product_group,
        sb_c_af.asin,

        -- Parent Code
        case
            when sb_c_af.tenant_id = 1
                then u_bb_pc.parent_code
            when sb_c_af.tenant_id = 2
                then ry_pc.parent_code
        end as parent_code,

        -- Portfolio Code
        case
            when sb_c_af.tenant_id = 1
                then COALESCE(u_bb_pc.portfolio_code, sb_c_af.portfolio_name)
        end as portfolio_code,

        -- Product Code
        case
            when sb_c_af.tenant_id = 1
                then sb_c_af.product_code
            when sb_c_af.tenant_id = 2
                then ry_pc.product_code
        end as product_code,

        -- Product Color
        case
            when sb_c_af.tenant_id = 2
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(0)])
        end as product_color,

        -- Product Pack Size
        case
            when sb_c_af.tenant_id = 2
                then TRIM(SPLIT(ry_pc.shaker_code, "_")[SAFE_OFFSET(1)])
        end as product_pack_size

    from sb_campaigns_with_added_fields as sb_c_af

    left join unique_bb_product_codes as u_bb_pc
        on sb_c_af.product_code = u_bb_pc.product_code

    left join rymora_product_codes as ry_pc
        on sb_c_af.asin = ry_pc.asin

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
        impressions,
        clicks,
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        top_of_search_impression_share,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        product_group,
        asin,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,

        -- Parent Code
        case
            when tenant_id = 1
                then COALESCE(parent_code, TRIM(SPLIT(portfolio_code, "-")[SAFE_OFFSET(0)]))
            when tenant_id = 2
                then parent_code
        end as parent_code

    from get_sb_product_codes

)

select * from get_sb_parent_codes
