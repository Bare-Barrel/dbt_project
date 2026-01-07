-- int_calculate_fields_for_sp_placement.sql sp_cp_03

{{ config(materialized='ephemeral') }}

with

sp_placements_with_portfolio as (

    select * from {{ ref('int_get_sp_placement_portfolio') }}

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
        tenant_id,
        impressions,
        clicks,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,

        -- Click-through Rate
        SAFE_DIVIDE(click_through_rate, 100) as click_through_rate,

        -- Conversion Rate
        SAFE_DIVIDE(purchases_14d, clicks) as conversion_rate,

        -- Campaign Placement Classification
        case
            when placement_classification = "Top of Search on-Amazon"
                then "TOP OF SEARCH ON-AMAZON (TOS)"
            {# when placement_classification = "Other on-Amazon"
                then "OTHER ON-AMAZON (ROS)" #}
            when placement_classification = "Detail Page on-Amazon"
                then "DETAIL PAGE ON-AMAZON (PP)"
            when placement_classification = "Rest of Search"
                then "REST OF SEARCH (ROS)"
            else UPPER(placement_classification)
        end as placement_classification,

        -- target product
        case
            when tenant_id = 2
                then TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)])
        end as target_product,

        -- Parent Code
        case
            when tenant_id = 1
                then
                    case
                        when REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(1)]), r"^[A-Z]{4}$")
                            then TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(1)])
                    end
        end as parent_code,

        -- Product Code for Bare Barrel only
        case
            when tenant_id = 1
                then
                    case
                        when
                            REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)]), r"^[A-Z]{3}$")
                            or REGEXP_CONTAINS(TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)]), r"^BTS|ATS")
                            then TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)])
                    end
        end as product_code

    from sp_placements_with_portfolio

),

get_product_color as (

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
        placement_classification,
        impressions,
        clicks,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        click_through_rate,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,
        conversion_rate,
        target_product,
        parent_code,
        product_code,

        -- Product Color
        case
            when tenant_id = 2
                then
                    case
                        when REGEXP_CONTAINS(target_product, r".*Blk$")
                            then "BLACK"
                        when REGEXP_CONTAINS(target_product, r".*Pnk$")
                            then "PINK"
                        when REGEXP_CONTAINS(target_product, r".*Gry$")
                            then "GREY"
                        when
                            REGEXP_CONTAINS(target_product, r".*Rse$")
                            or REGEXP_CONTAINS(target_product, r".*Rcl$")
                            then "ROSE CORAL"
                        when REGEXP_CONTAINS(target_product, r".*Chr$")
                            then "CHARCOAL"
                        when REGEXP_CONTAINS(target_product, r".*P&T$")
                            then "PURPLE/TEAL"
                        when REGEXP_CONTAINS(target_product, r".*N&G$")
                            then "NAVY/GREY"
                    end
        end as product_color

    from calculate_fields

)

select * from get_product_color
