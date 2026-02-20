-- stg_sponsored_products__campaign.sql

with

sp_campaign as (

    select * from {{ source('sponsored_products', 'campaign') }}

),

rename_fields as (

    select
        -- ids
        date as campaign_date,
        created_at,
        updated_at,
        campaign_id,
        tenant_id,
        campaign_applicable_budget_rule_id,
        marketplace,
        campaign_name,
        campaign_status,
        campaign_budget_type,
        campaign_bidding_strategy,
        campaign_budget_currency_code,
        campaign_applicable_budget_rule_name,
        impressions,
        cost,
        spend,
        clicks,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        purchases_same_sku_1d,
        purchases_same_sku_7d,
        purchases_same_sku_14d,
        purchases_same_sku_30d,
        cost_per_click,
        click_through_rate,
        top_of_search_impression_share,
        campaign_budget_amount,
        campaign_rule_based_budget_amount,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d

    from sp_campaign

),

cast_data_types as (

    select
        -- ids
        CAST(campaign_id as string) as campaign_id,
        tenant_id,
        campaign_applicable_budget_rule_id,

        -- strings
        marketplace,
        campaign_name,
        campaign_status,
        campaign_budget_type,
        campaign_bidding_strategy,
        campaign_budget_currency_code,
        campaign_applicable_budget_rule_name,

        -- numerics
        impressions,
        cost,
        spend,
        clicks,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        purchases_same_sku_1d,
        purchases_same_sku_7d,
        purchases_same_sku_14d,
        purchases_same_sku_30d,
        cost_per_click,
        click_through_rate,
        top_of_search_impression_share,
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
