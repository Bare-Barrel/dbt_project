-- stg_sponsored_products__search_term.sql

with

search_term_table as (

    select * from {{ source('sponsored_products', 'search_term') }}

),

rename_fields as (

    select
        date as record_date,
        created_at,
        updated_at,
        keyword_id,
        keyword,
        keyword_type,
        search_term,
        targeting,
        match_type,
        ad_group_id,
        ad_group_name,
        ad_keyword_status,
        campaign_id,
        campaign_name,
        campaign_status,
        campaign_budget_type,
        campaign_budget_currency_code,
        portfolio_id,
        marketplace,
        tenant_id,
        impressions,
        clicks,
        cost,
        keyword_bid,
        campaign_budget_amount,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        units_sold_other_sku_7d,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        sales_other_sku_7d,
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
        click_through_rate,
        roas_clicks_7d,
        roas_clicks_14d,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d

    from search_term_table

),

cast_data_types as (

    select
        -- ids
        CAST(keyword_id as string) as keyword_id,
        CAST(ad_group_id as string) as ad_group_id,
        CAST(campaign_id as string) as campaign_id,
        CAST(portfolio_id as string) as portfolio_id,
        tenant_id,

        -- strings
        keyword,
        keyword_type,
        search_term,
        targeting,
        match_type,
        ad_group_name,
        ad_keyword_status,
        campaign_name,
        campaign_status,
        campaign_budget_type,
        campaign_budget_currency_code,
        marketplace,

        -- numerics
        impressions,
        clicks,
        cost,
        keyword_bid,
        campaign_budget_amount,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        units_sold_same_sku_1d,
        units_sold_same_sku_7d,
        units_sold_same_sku_14d,
        units_sold_same_sku_30d,
        units_sold_other_sku_7d,
        sales_1d,
        sales_7d,
        sales_14d,
        sales_30d,
        sales_other_sku_7d,
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
        click_through_rate,
        roas_clicks_7d,
        roas_clicks_14d,
        attributed_sales_same_sku_1d,
        attributed_sales_same_sku_7d,
        attributed_sales_same_sku_14d,
        attributed_sales_same_sku_30d,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d,

        -- datetime
        record_date,
        created_at,
        updated_at

    from rename_fields

)

select * from cast_data_types
