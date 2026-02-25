-- stg_sponsored_display__campaigns.sql

with

sd_campaigns as (

    select * from {{ source('sponsored_display', 'campaigns') }}

),

rename_fields as (

    select
        start_date,
        created_at,
        updated_at,
        campaign_id,
        name as campaign_name,
        state as campaign_state,
        portfolio_id,
        marketplace,
        tenant_id,
        cost_type,
        budget_type,
        tactic,
        delivery_profile,
        budget

    from sd_campaigns

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
        cost_type,
        budget_type,
        tactic,
        delivery_profile,

        -- numerics
        budget,

        -- datetime
        start_date,
        created_at,
        updated_at

    from rename_fields

),

handle_nulls as (

    select
        start_date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_state,
        marketplace,
        tenant_id,
        cost_type,
        budget_type,
        tactic,
        delivery_profile,
        budget,

        COALESCE(portfolio_id, "0") as portfolio_id

    from cast_data_types

)

select * from handle_nulls
