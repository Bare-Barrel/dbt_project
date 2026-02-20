-- obt_sp_campaigns.sql
-- Almost same as SP portfolio_performance_summary_view / SP Historical Campaign Cost

with

agg_sp_campaigns as (

    select * from {{ ref('int_kpi_tracker__aggregate_sp_campaigns') }}

),

agg_sp_campaign_console as (

    select * from {{ ref('int_kpi_tracker__aggregate_sp_cc') }}

),

union_all_sp_campaigns as (

    select
        campaign_date,
        tenant_id,
        marketplace,
        portfolio_code,
        total_impressions,
        total_clicks,
        total_cost_usd,
        total_units_sold_clicks_7d,
        total_sales_7d_usd
    from agg_sp_campaigns

    union all

    select
        campaign_date,
        tenant_id,
        marketplace,
        portfolio_code,
        total_impressions,
        total_clicks,
        total_spend_usd as total_cost_usd,
        _7_day_total_orders as total_units_sold_clicks_7d,
        _7_day_total_sales_usd as total_sales_7d_usd
    from agg_sp_campaign_console

)

select * from union_all_sp_campaigns
