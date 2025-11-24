-- obt_sb_campaigns.sql
-- API Data source v3 (2023-09-21 - present)

with

int_sb_campaigns as (

    select * from {{ ref('int_calculate_fields_for_sb_campaigns') }}

),

select_and_reorder_fields as (

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
        top_of_search_impression_share,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        {# placement_classification, #}
        product_group

    from int_sb_campaigns

)

select * from select_and_reorder_fields
