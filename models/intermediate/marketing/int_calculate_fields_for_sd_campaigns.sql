-- int_calculate_fields_for_sd_campaigns.sql sd_03

{{ config(materialized='ephemeral') }}

with

sd_campaigns_with_portfolio as (

    select * from {{ ref('int_get_sd_portfolio') }}

),

calculate_fields as (

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
        purchases_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,

        -- Cost per Click
        SAFE_DIVIDE(cost_usd, clicks) as cost_per_click_usd,

        -- Click-through Rate
        SAFE_DIVIDE(clicks, impressions) as click_through_rate,

        -- Conversion Rate
        SAFE_DIVIDE(purchases_clicks, clicks) as conversion_rate,

        -- ASIN
        case
            when REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(2)]), r"^[A-Z0-9]{10}$")
                then TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(2)])
        end as asin_string1,

        case
            when REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(3)]), r"^[A-Z0-9]{10}$")
                then TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(3)])
        end as asin_string2,

        case
            when REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(4)]), r"^[A-Z0-9]{10}$")
                then TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(4)])
        end as asin_string3

        {# case
            when REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(5)]), r"^[A-Z0-9]{10}$")
                then TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(5)])
        end as asin_string4 #}

    from sd_campaigns_with_portfolio

),

get_primary_asin as (

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
        purchases_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,

        COALESCE(asin_string1, asin_string2, asin_string3) as asin

    from calculate_fields

)

{# handle_edge_case as (

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
        purchases_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,

        case
            when STRPOS(campaign_name, "B07ZHHVSY6") != 0
                then "B07ZHHVSY6"
            else asin
        end as asin

    from get_primary_asin

) #}

select * from get_primary_asin
