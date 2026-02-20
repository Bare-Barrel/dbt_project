-- stg_sponsored_products__campaigns.sql

with

sp_campaigns as (

    select * from {{ source('sponsored_products', 'campaigns') }}

),

rename_fields as (

    select
        campaign_id,
        portfolio_id,
        tenant_id,
        name as campaign_name,
        state as campaign_state,
        marketplace,
        targeting_type,
        budget_budget_type as budget_type,
        budget_budget as budget,
        dynamic_bidding_strategy,
        dynamic_bidding_placement_bidding,
        start_date,
        end_date,
        created_at,
        updated_at

    from sp_campaigns

),

cast_data_types as (

    select
        -- ids
        CAST(campaign_id as string) as campaign_id,
        CAST(portfolio_id as string) as portfolio_id,
        tenant_id,

        -- strings
        campaign_name,
        campaign_state,
        marketplace,
        targeting_type,
        budget_type,
        dynamic_bidding_strategy,
        dynamic_bidding_placement_bidding,

        -- numerics
        budget,

        -- datetime
        start_date,
        end_date,
        created_at,
        updated_at

    from rename_fields

),

handle_nulls as (

    select
        campaign_id,
        tenant_id,
        campaign_name,
        campaign_state,
        marketplace,
        targeting_type,
        budget_type,
        budget,
        dynamic_bidding_strategy,
        dynamic_bidding_placement_bidding,
        start_date,
        end_date,
        created_at,
        updated_at,

        COALESCE(portfolio_id, "0") as portfolio_id

    from cast_data_types

)

select * from handle_nulls
