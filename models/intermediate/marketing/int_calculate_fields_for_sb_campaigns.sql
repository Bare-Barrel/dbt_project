-- int_calculate_fields_for_sb_campaigns.sql

{{ config(materialized='view') }}

with

joined_campaign_and_campaign_placement as (

    select * from {{ ref('int_get_sb_placement_and_portfolio') }}

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
        SAFE_DIVIDE(cost_usd, clicks) as cost_per_click,

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
        end as product_group

    from joined_campaign_and_campaign_placement

)

select * from calculate_fields
