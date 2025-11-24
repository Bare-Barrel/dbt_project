-- obt_sp_campaigns.sql
-- API data source. (04-02-2023 to present)

with

int_sp_campaigns as (

    select * from {{ ref('int_calculate_fields_for_sp_campaigns') }}

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
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        click_through_rate,
        top_of_search_impression_share,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,
        conversion_rate,
        {# placement_classification, #}
        target_product,
        product_description,
        product_group,
        product_color

    from int_sp_campaigns

)

select * from select_and_reorder_fields
