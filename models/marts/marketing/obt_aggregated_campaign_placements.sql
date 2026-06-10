-- obt_aggregated_campaign_placements.sql

with

agg_unioned_campaign_placements as (

    select * from {{ ref('int_aggregate_unioned_campaign_placements') }}

),

reorder_fields as (

    select
        record_date,
        marketplace,
        asin,
        tenant_id,
        campaign_status,
        placement_classification,
        ad_type,
        sb_ad_type,
        impressions,
        clicks,
        units_sold_clicks,
        purchases_clicks,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd

    from agg_unioned_campaign_placements

)

select * from reorder_fields
