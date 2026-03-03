-- obt_sp_advertised_products.sql   TODO: remove fields that are not needed

with

sp_advertised_product_usd as (

    select * from {{ ref('int_convert_sp_advertised_product_amounts_to_usd') }}

),

convert_ids_to_int as ( -- Optimize import compression in Power BI

    select
        campaign_date,
        created_at,
        updated_at,
        CAST(ad_id as integer) as ad_id,
        CAST(ad_group_id as integer) as ad_group_id,
        CAST(campaign_id as integer) as campaign_id,
        CAST(portfolio_id as integer) as portfolio_id,
        tenant_id,
        ad_group_name,
        campaign_name,
        campaign_status,
        marketplace,
        {# advertised_sku, #}
        advertised_asin,
        {# campaign_budget_type, #}
        {# campaign_budget_currency_code, #}
        impressions,
        cost_usd,
        spend_usd,
        clicks,
        sales_14d_usd,
        purchases_14d,
        purchases_same_sku_14d,
        acos_clicks_14d,
        cost_per_click_usd,
        roas_clicks_14d,
        click_through_rate,
        units_sold_clicks_14d,
        units_sold_same_sku_14d,
        campaign_budget_amount_usd,
        attributed_sales_same_sku_14d_usd,
        kindle_edition_normalized_pages_read_14d,
        kindle_edition_normalized_pages_royalties_14d

    from sp_advertised_product_usd

)

select * from convert_ids_to_int
