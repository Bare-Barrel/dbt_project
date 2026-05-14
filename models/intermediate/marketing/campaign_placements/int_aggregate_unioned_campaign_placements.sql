-- int_aggregate_unioned_campaign_placements.sql sp_sb_cp_07

{{ config(materialized='view') }}

with

unioned_campaign_placements_asin as (

    select * from {{ ref('int_union_campaign_placements_with_asin') }}

),

aggregate_campaign_placements as (

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

    from unioned_campaign_placements_asin

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

select * from aggregate_campaign_placements
