-- stg_amazon_ads_api__amazon_advertising_portfolios_v3.sql
-- Retains only the most recent portfolio list

with

amazon_advertising_portfolio_v3 as (

    select * from {{ source('amazon_ads_api', 'amazon_advertising_portfolios_v3') }}

),

rename_fields as (

    select
        inbudget as in_budget,
        name as portfolio_name,
        portfolioid as portfolio_id,
        state as portfolio_state,
        budget_currencycode as budget_currency_code,
        budget_policy,
        budgetcontrols_campaignunspentbudgetsharing_featurestate as budget_controls_campaign_unspent_budget_sharing_feature_state,
        marketplace,
        tenant_id,
        budget_amount,
        recorded_at

    from amazon_advertising_portfolio_v3

),

cast_data_types as (

    select
        -- ids
        CAST(portfolio_id as string) as portfolio_id,
        tenant_id,

        -- strings
        portfolio_name,
        portfolio_state,
        budget_currency_code,
        budget_policy,
        budget_controls_campaign_unspent_budget_sharing_feature_state,
        marketplace,

        -- numerics
        CAST(budget_amount as numeric) as budget_amount,

        --boolean
        in_budget,

        -- datetime
        recorded_at

    from rename_fields

),

retain_latest_recorded_date as (

    select *
    from cast_data_types
    where DATE(recorded_at) = (
        select MAX(DATE(cdt.recorded_at))
        from cast_data_types as cdt
    )

)

select * from retain_latest_recorded_date
