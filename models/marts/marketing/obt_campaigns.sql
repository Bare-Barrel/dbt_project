-- obt_campaigns.sql

with

int_sp_campaigns as (

    select * from {{ ref('int_calculate_fields_for_sp_campaigns') }}

),

int_sb_campaigns as (

    select * from {{ ref('int_calculate_fields_for_sb_campaigns') }}

),

int_sd_campaigns as (

    select * from {{ ref('int_calculate_fields_for_sd_campaigns') }}

),

select_sp_fields as (

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
        product_group,
        product_color
    {# placement_classification, #}

    from int_sp_campaigns

),

select_sb_fields as (

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
        sales_clicks_usd,
        campaign_budget_amount_usd,
        cost_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        top_of_search_impression_share,
        tenant_id,
        "SPONSORED BRANDS" as ad_type,
        product_group,
        CAST(null as string) as product_color
    {# placement_classification, #}

    from int_sb_campaigns

),

select_sd_fields as (

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
        sales_clicks_usd,
        campaign_budget_amount_usd,
        cost_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        null as top_of_search_impression_share,
        tenant_id,
        "SPONSORED DISPLAY" as ad_type,
        product_group,
        CAST(null as string) as product_color

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
