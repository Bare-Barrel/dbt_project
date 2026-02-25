-- stg_sponsored_products__purchased_product.sql

with

sp_purchased_product as (

    select * from {{ source('sponsored_products', 'purchased_product') }}

),

rename_fields as (

    select
        date as campaign_date,
        created_at,
        updated_at,
        ad_group_id,
        ad_group_name,
        campaign_id,
        campaign_name,
        portfolio_id,
        advertised_sku,
        advertised_asin,
        purchased_asin,
        keyword_id,
        keyword,
        keyword_type,
        match_type,
        marketplace,
        tenant_id,
        campaign_budget_currency_code,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        sales_other_sku_1d,
        sales_other_sku_7d,
        sales_other_sku_14d,
        sales_other_sku_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        purchases_other_sku_1d,
        purchases_other_sku_7d,
        purchases_other_sku_14d,
        purchases_other_sku_30d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_other_sku_1d,
        units_sold_other_sku_7d,
        units_sold_other_sku_14d,
        units_sold_other_sku_30d,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d

    from sp_purchased_product

),

cast_data_types as (

    select
        -- ids
        CAST(ad_group_id as string) as ad_group_id,
        CAST(campaign_id as string) as campaign_id,
        CAST(portfolio_id as string) as portfolio_id,
        CAST(keyword_id as string) as keyword_id,
        tenant_id,

        -- strings
        ad_group_name,
        campaign_name,
        advertised_sku,
        advertised_asin,
        purchased_asin,
        keyword,
        keyword_type,
        match_type,
        marketplace,
        campaign_budget_currency_code,

        -- numerics
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        sales_other_sku_1d,
        sales_other_sku_7d,
        sales_other_sku_14d,
        sales_other_sku_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        purchases_other_sku_1d,
        purchases_other_sku_7d,
        purchases_other_sku_14d,
        purchases_other_sku_30d,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_other_sku_1d,
        units_sold_other_sku_7d,
        units_sold_other_sku_14d,
        units_sold_other_sku_30d,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d,

        -- datetime
        campaign_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
