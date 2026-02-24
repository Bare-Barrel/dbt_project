-- stg_sponsored_brands__campaigns.sql

with

sb_campaigns as (

    select * from {{ source('sponsored_brands', 'campaigns') }}

),

rename_fields as (

    select
        start_date,
        created_at,
        updated_at,
        campaign_id,
        name as campaign_name,
        state as campaign_state,
        marketplace,
        portfolio_id,
        brand_entity_id,
        tenant_id,
        outcome,
        cost_type,
        budget_type,
        bidding_bid_optimization_strategy,
        bidding_bid_optimization,
        is_multi_ad_groups_enabled,
        budget

    from sb_campaigns

),

cast_data_types as (

    select
        -- ids
        CAST(campaign_id as string) as campaign_id,
        CAST(portfolio_id as string) as portfolio_id,
        CAST(brand_entity_id as string) as brand_entity_id,
        tenant_id,

        -- strings
        campaign_name,
        campaign_state,
        marketplace,
        outcome,
        cost_type,
        budget_type,
        bidding_bid_optimization_strategy,

        -- numerics
        budget,

        -- boolean
        bidding_bid_optimization,
        is_multi_ad_groups_enabled,

        -- datetime
        start_date,
        created_at,
        updated_at

    from rename_fields

),

handled_nulls as (

    select
        campaign_id,
        brand_entity_id,
        tenant_id,
        campaign_name,
        campaign_state,
        marketplace,
        outcome,
        cost_type,
        budget_type,
        bidding_bid_optimization_strategy,
        budget,
        bidding_bid_optimization,
        is_multi_ad_groups_enabled,
        start_date,
        created_at,
        updated_at,

        COALESCE(portfolio_id, "0") as portfolio_id

    from cast_data_types
)

select * from handled_nulls
