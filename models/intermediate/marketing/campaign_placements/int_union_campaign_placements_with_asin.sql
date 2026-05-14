-- int_union_campaign_placements_with_asin.sql sp_sb_cp_06

{{ config(materialized='ephemeral') }}

with

sp_placement_asin as (

    select * from {{ ref('int_get_sp_placement_asin') }}

),

sb_placement_asin as (

    select * from {{ ref('int_get_sb_placement_asin') }}

),

select_sp_placement as (

    select
        campaign_date as record_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        marketplace,
        placement_classification,
        tenant_id,
        "SPONSORED PRODUCTS" as ad_type,
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,
        asin,
        impressions,
        clicks,
        units_sold_clicks_14d as units_sold_clicks,
        purchases_14d as purchases_clicks,
        click_through_rate,
        conversion_rate,
        campaign_budget_amount_usd,
        cost_usd,
        sales_14d_usd as sales_clicks_usd,
        cost_per_click_usd,
        "NOT APPLICABLE" as sb_ad_type

    from sp_placement_asin

),

select_sb_placement as (

    select
        campaign_date as record_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        marketplace,
        placement_classification,
        tenant_id,
        "SPONSORED BRANDS" as ad_type,
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,
        asin,
        impressions,
        clicks,
        units_sold_clicks,
        purchases_clicks,
        click_through_rate,
        conversion_rate,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        cost_per_click_usd,
        sb_ad_type

    from sb_placement_asin

),

combine_sp_and_sb_placement as (

    select * from select_sp_placement

    union all

    select * from select_sb_placement

)

select * from combine_sp_and_sb_placement
