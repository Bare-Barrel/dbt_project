-- stg_sponsored_products__advertised_product.sql

with

sp_advertised_product as (

    select * from {{ source('sponsored_products', 'advertised_product') }}

),

rename_fields as (

    select
        date as campaign_date,
        created_at,
        updated_at,
        ad_id,
        ad_group_id,
        ad_group_name,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        marketplace,
        advertised_sku,
        advertised_asin,
        tenant_id,
        campaign_budget_type,
        campaign_budget_currency_code,
        impressions,
        cost,
        spend,
        clicks,
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
        acos_clicks_7d,
        acos_clicks_14d,
        cost_per_click,
        roas_clicks_7d,
        roas_clicks_14d,
        click_through_rate,
        sales_other_sku_7d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        units_sold_other_sku_7d,
        campaign_budget_amount,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d

    from sp_advertised_product

),

cast_data_types as (

    select
        -- ids
        CAST(ad_id as string) as ad_id,
        CAST(ad_group_id as string) as ad_group_id,
        CAST(campaign_id as string) as campaign_id,
        CAST(portfolio_id as string) as portfolio_id,
        tenant_id,

        -- strings
        ad_group_name,
        campaign_name,
        campaign_status,
        marketplace,
        advertised_sku,
        advertised_asin,
        campaign_budget_type,
        campaign_budget_currency_code,

        -- numerics
        impressions,
        cost,
        spend,
        clicks,
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
        acos_clicks_7d,
        acos_clicks_14d,
        cost_per_click,
        roas_clicks_7d,
        roas_clicks_14d,
        click_through_rate,
        sales_other_sku_7d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        units_sold_other_sku_7d,
        campaign_budget_amount,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
