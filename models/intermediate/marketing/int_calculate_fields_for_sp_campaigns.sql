-- int_calculate_fields_for_sp_campaigns.sql

{{ config(materialized='view') }}

with

campaigns_with_placement_and_portfolio as (

    select * from {{ ref('int_get_sp_placement_and_portfolio') }}

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
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        click_through_rate,
        top_of_search_impression_share,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,

        -- Conversion Rate
        SAFE_DIVIDE(purchases_14d, clicks) as conversion_rate,

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

        -- target product
        case
            when tenant_id = 2
                then TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(0)])
        end as target_product,

        -- Product Description
        case
            when tenant_id = 2
                then TRIM(SPLIT(campaign_name, "|")[SAFE_OFFSET(2)])
        end as product_description,

        -- Product Group
        case
            when tenant_id = 2
                then SPLIT(TRIM(SPLIT(campaign_name, "|")[OFFSET(0)]), "-")[OFFSET(0)]
        end as product_group

    from campaigns_with_placement_and_portfolio

),

standardize_product_group as (

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
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        click_through_rate,
        top_of_search_impression_share,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,
        conversion_rate,
        {# placement_classification, #}
        target_product,
        product_description,

        -- Standardize product_group
        case
            when
                REGEXP_CONTAINS(product_group, r"^ElbSlv")
                or REGEXP_CONTAINS(product_group, r"^ElbowSleeve")
                then "ELBOW SLEEVE"
            when
                REGEXP_CONTAINS(product_group, r"^Comp Sleeve")
                or REGEXP_CONTAINS(product_group, r"^CompSock")
                or REGEXP_CONTAINS(product_group, r"^CmpSk")
                then "COMPRESSION SOCKS"
            when
                REGEXP_CONTAINS(product_group, r"^KneeSleeve")
                or REGEXP_CONTAINS(product_group, r"^KnSlv")
                then "KNEE SLEEVE"
            when REGEXP_CONTAINS(product_group, r"^HikSk")
                then "HIKING SOCKS"
            when REGEXP_CONTAINS(product_group, r"^elbow/knee sleeves")
                then "ELBOW/KNEE SLEEVES"
            when
                REGEXP_CONTAINS(product_group, r"^Calf C Socks")
                or REGEXP_CONTAINS(product_group, r"^Calf C Sleeves")
                or REGEXP_CONTAINS(product_group, r"^ClfSlv")
                or REGEXP_CONTAINS(product_group, r"^calf compression sleeves")
                then "CALF SLEEVES"
            when
                REGEXP_CONTAINS(product_group, r"^PfSk")
                or REGEXP_CONTAINS(product_group, r"^Plantar Socks")
                then "PF SOCKS"
            when REGEXP_CONTAINS(product_group, r"^GrpSk")
                then "GRIP SOCKS"
            else UPPER(product_group)
        end as product_group,

        -- Product Color
        case
            when REGEXP_CONTAINS(target_product, r".*Blk$")
                then "BLACK"
            when REGEXP_CONTAINS(target_product, r".*Pnk$")
                then "PINK"
            when REGEXP_CONTAINS(target_product, r".*Gry$")
                then "GREY"
            when REGEXP_CONTAINS(target_product, r".*Rse$")
                then "ROSE"
            when REGEXP_CONTAINS(target_product, r".*Chr$")
                then "CHARCOAL"
        end as product_color

    from calculate_fields

)

select * from standardize_product_group
