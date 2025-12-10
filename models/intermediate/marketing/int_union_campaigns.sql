-- int_union_campaigns.sql c_05

{{ config(materialized='view') }}

with

int_sp_campaigns as (

    select * from {{ ref('int_get_sp_product_codes') }}

),

int_sb_campaigns as (

    select * from {{ ref('int_get_sb_product_codes') }}

),

int_sd_campaigns as (

    select * from {{ ref('int_get_sd_product_codes') }}

),

select_sp_fields as ( -- API data source. (04-02-2023 to present)

    select
        date as record_date,
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
        units_sold_clicks_14d as units_sold_clicks,
        sales_14d_usd as sales_clicks_usd,
        campaign_budget_amount_usd,
        cost_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        top_of_search_impression_share,
        tenant_id,
        "SPONSORED PRODUCTS" as ad_type,
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size

    from int_sp_campaigns

),

select_sb_fields as ( -- API Data source v3 (2023-09-21 - present)

    select
        date as record_date,
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
        sales_clicks_usd,
        campaign_budget_amount_usd,
        cost_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        top_of_search_impression_share,
        tenant_id,
        "SPONSORED BRANDS" as ad_type,
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size

    from int_sb_campaigns

),

select_sd_fields as ( -- API v3 (2025-01-11 - present)

    select
        date as record_date,
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
        sales_clicks_usd,
        campaign_budget_amount_usd,
        cost_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        null as top_of_search_impression_share,
        tenant_id,
        "SPONSORED DISPLAY" as ad_type,
        parent_code,
        CAST(null as string) as portfolio_code,
        product_code,
        product_color,
        product_pack_size

    from int_sd_campaigns

),

union_all as (

    select * from select_sp_fields

    union all

    select * from select_sb_fields

    union all

    select * from select_sd_fields

)

select * from union_all
