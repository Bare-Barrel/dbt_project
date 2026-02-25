-- stg_sponsored_display__campaign_console.sql

with

sd_campaign_console as (

    select * from {{ source('sponsored_display', 'campaign_console') }}

),

rename_fields as (

    select
        date as campaign_date,
        created_at,
        updated_at,
        campaign_name,
        status as campaign_status,
        portfolio_name,
        marketplace,
        tenant_id,
        cost_type,
        currency,
        impressions,
        clicks,
        spend,
        budget,
        cost_per_click,
        click_thru_rate,
        _14_day_total_sales,
        _14_day_total_sales_click,
        _14_day_total_units,
        _14_day_total_units_click,
        _14_day_total_orders,
        _14_day_total_orders_click,
        _14_day_detail_page_views,
        _14_day_new_to_brand_sales,
        _14_day_new_to_brand_sales_click,
        _14_day_new_to_brand_units,
        _14_day_new_to_brand_units_click,
        _14_day_new_to_brand_orders,
        _14_day_new_to_brand_orders_click,
        total_advertising_cost_of_sales,
        total_advertising_cost_of_sales_click,
        total_return_on_advertising_spend,
        total_return_on_advertising_spend_click,
        viewable_impressions,
        cost_per_1_000_viewable_impressions

    from sd_campaign_console

)

select * from rename_fields
