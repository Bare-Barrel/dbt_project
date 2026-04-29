-- stg_sponsored_products__campaign_placement.sql

with

sp_campaign_placement as (

    select * from {{ source('sponsored_products', 'campaign_placement') }}

),

rename_fields as (

    select
        date as campaign_date,
        campaign_id,
        campaign_name,
        campaign_status,
        campaign_applicable_budget_rule_id,
        campaign_applicable_budget_rule_name,
        campaign_budget_type,
        campaign_budget_currency_code,
        campaign_bidding_strategy,
        placement_classification,
        marketplace,
        tenant_id,
        impressions,
        clicks,
        cost,
        spend,
        cost_per_click,
        click_through_rate,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        purchases_same_sku_1d,
        purchases_same_sku_7d,
        purchases_same_sku_14d,
        purchases_same_sku_30d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        campaign_budget_amount,
        campaign_rule_based_budget_amount,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d,
        created_at,
        updated_at

    from sp_campaign_placement

),

cast_data_types as (

    select
        -- ids
        CAST(campaign_id as string) as campaign_id,
        CAST(campaign_applicable_budget_rule_id as string) as campaign_applicable_budget_rule_id,
        tenant_id,

        -- strings
        marketplace,
        campaign_name,
        campaign_status,
        campaign_applicable_budget_rule_name,
        campaign_budget_type,
        campaign_budget_currency_code,
        campaign_bidding_strategy,
        placement_classification,

        -- numerics
        impressions,
        clicks,
        cost,
        spend,
        cost_per_click,
        click_through_rate,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        purchases_same_sku_1d,
        purchases_same_sku_7d,
        purchases_same_sku_14d,
        purchases_same_sku_30d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        campaign_budget_amount,
        campaign_rule_based_budget_amount,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
