-- obt_aggregated_campaign_placements_sb.sql

with

sb_placement_asin as (

    select * from {{ ref('int_get_sb_placement_asin') }}

),

select_sb_placement as (

    select
        date as record_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        marketplace,
        placement_classification,
        tenant_id,
        "SPONSORED BRANDS" as ad_type,
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,
        asin,
        impressions,
        clicks,
        units_sold_clicks,
        purchases_clicks,
        click_through_rate,
        conversion_rate,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        cost_per_click_usd,
        sb_ad_type

    from sb_placement_asin

),

aggregate_campaign_placements_sb as (

    select
        record_date,
        marketplace,
        asin,
        tenant_id,
        campaign_status,
        placement_classification,
        ad_type,
        sb_ad_type,

        SUM(impressions) as impressions,
        SUM(clicks) as clicks,
        SUM(units_sold_clicks) as units_sold_clicks,
        SUM(purchases_clicks) as purchases_clicks,
        SUM(campaign_budget_amount_usd) as campaign_budget_amount_usd,
        SUM(cost_usd) as cost_usd,
        SUM(sales_clicks_usd) as sales_clicks_usd

    from select_sb_placement

    where sb_ad_type = "SB"

    group by
        record_date,
        marketplace,
        asin,
        tenant_id,
        campaign_status,
        placement_classification,
        ad_type,
        sb_ad_type

)

select * from aggregate_campaign_placements_sb
