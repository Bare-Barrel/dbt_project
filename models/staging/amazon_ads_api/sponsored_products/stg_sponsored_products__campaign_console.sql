-- stg_sponsored_products__campaign_console.sql

with

sp_campaign_console as (

    select * from {{ source('sponsored_products', 'campaign_console') }}

),

rename_fields as (

    select
        -- ids
        tenant_id,

        -- strings
        campaign_name,
        campaign_type,
        status as campaign_status,
        portfolio_name,
        targeting_type,
        bidding_strategy,
        marketplace,
        country,
        currency,

        -- numerics
        spend,
        budget,
        impressions,
        clicks,
        _7_day_total_sales,
        _7_day_total_orders,
        cost_per_click,
        click_thru_rate,
        last_year_spend,
        last_year_impressions,
        last_year_clicks,
        last_year_cost_per_click,
        total_advertising_cost_of_sales,
        total_return_on_advertising_spend,

        -- datetime
        date as campaign_date,
        created_at,
        updated_at

    from sp_campaign_console

)

select * from rename_fields
