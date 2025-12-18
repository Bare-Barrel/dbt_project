-- int_union_all_campaigns.sql

{{ config(materialized='view') }}

with

all_campaigns as (

    select * from {{ ref('int_union_campaigns') }}

),

all_campaign_placements as (

    select * from {{ ref('int_union_campaign_placements') }}

),

select_fields_for_campaign_placements as (

    select
        record_date,
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
        ad_type,
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,
        impressions,
        clicks,
        units_sold_clicks,
        sales_clicks_usd,
        campaign_budget_amount_usd,
        cost_usd,
        cost_per_click_usd,
        click_through_rate,
        conversion_rate,
        CAST(null as numeric) as top_of_search_impression_share

    from all_campaign_placements

),

get_rows_from_campaigns as (

    select
        c.record_date,
        c.created_at,
        c.updated_at,
        c.campaign_id,
        c.campaign_name,
        c.campaign_status,
        c.portfolio_id,
        c.portfolio_name,
        c.marketplace,
        CAST(null as string) as placement_classification,
        c.tenant_id,
        c.ad_type,
        c.parent_code,
        c.portfolio_code,
        c.product_code,
        c.product_color,
        c.product_pack_size,
        c.impressions,
        c.clicks,
        c.units_sold_clicks,
        c.sales_clicks_usd,
        c.campaign_budget_amount_usd,
        c.cost_usd,
        c.cost_per_click_usd,
        c.click_through_rate,
        c.conversion_rate,
        c.top_of_search_impression_share

    from all_campaigns as c

    where not exists (
        select 1
        from select_fields_for_campaign_placements as e
        where
            e.campaign_id = c.campaign_id
            and e.record_date = c.record_date
    )

),

union_all_campaigns as (

    select * from get_rows_from_campaigns

    union all

    select * from select_fields_for_campaign_placements

)

select * from union_all_campaigns
