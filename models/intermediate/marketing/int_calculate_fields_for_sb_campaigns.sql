-- int_calculate_fields_for_sb_campaigns.sql sb_03

{{ config(materialized='ephemeral') }}

with

sb_with_portfolio as (

    select * from {{ ref('int_get_sb_portfolio') }}

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
        top_of_search_impression_share,
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

        -- Campaign Placement Classification
        {# case
            when placement_classification = "Top of Search on-Amazon"
                then "TOP OF SEARCH ON-AMAZON (TOS)"
            when placement_classification = "Other on-Amazon"
                then "OTHER ON-AMAZON (ROS)"
            when placement_classification = "Detail Page on-Amazon"
                then "DETAIL PAGE ON-AMAZON (PP)"
            else placement_classification
        end as placement_classification, #}

        -- Product Group
        case
            when tenant_id = 2
                then
                    case
                        when REGEXP_CONTAINS(campaign_name, r".*_B07YJ7QPXP_.*")
                            then "GRIP SOCKS"
                        when
                            REGEXP_CONTAINS(campaign_name, r".*_B07ZHJB1TK_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HFPJG8_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HG38CH_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZHJHQCF_.*")
                            then "CALF SLEEVES"
                    end
        end as product_group,

        -- Product Code for Bare Barrel only    TODO: use this to match with Bare Barrel master data
        case
            when tenant_id = 1
                then
                    case
                        when
                            REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)]), r"^[A-Z]{3}$")
                            or REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)]), r"\+")
                            then TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)])
                    end
        end as product_code,

        -- ASIN TODO: use this to match with Rymora master data B07YJ7QPXP, B081HFPJG8, B07ZHJB1TK
        -- TODO: match ASIN with Rymora master data??
        case
            when tenant_id = 2
                then TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(3)])
        end as asin

    from sb_with_portfolio

),

transform_product_code as (

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
        top_of_search_impression_share,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        product_group,
        asin,

        -- Simple transformation for product_code
        case
            when tenant_id = 1
                then
                    case
                        when REGEXP_CONTAINS(product_code, r"^.*\+$")
                            then REGEXP_REPLACE(product_code, r"[^A-Z]", "") -- remove digits
                        else product_code
                    end
        end as product_code

    from calculate_fields

)

select * from transform_product_code
