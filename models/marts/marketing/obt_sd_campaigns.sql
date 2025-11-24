-- obt_sd_campaigns.sql
-- API v3 (2025-01-11 - present)

with

int_sd_campaigns as (

    select * from {{ ref('int_calculate_fields_for_sd_campaigns') }}

),

select_and_reorder_fields as (

    select
        date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        marketplace,
        impressions,
        clicks,
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        product_group

    from int_sd_campaigns

)

select * from select_and_reorder_fields
