-- int_get_sp_product_codes.sql sp_04

{{ config(materialized='view') }}

with

sp_campaigns_with_added_fields as (

    select * from {{ ref('int_calculate_fields_for_sp_campaigns') }}

),

bb_product_codes as (

    select
        parent_code,
        portfolio_code,
        product_code

    from {{ ref('dim_product_codes') }}

    where tenant_id = 1

),

unique_bb_product_codes as (

    select distinct *
    from bb_product_codes

),

get_sp_product_codes as (

    select
        sp_c_af.date,
        sp_c_af.created_at,
        sp_c_af.updated_at,
        sp_c_af.campaign_id,
        sp_c_af.campaign_name,
        sp_c_af.campaign_status,
        sp_c_af.portfolio_id,
        sp_c_af.portfolio_name,
        sp_c_af.marketplace,
        sp_c_af.impressions,
        sp_c_af.clicks,
        sp_c_af.units_sold_clicks_1d,
        sp_c_af.units_sold_clicks_7d,
        sp_c_af.units_sold_clicks_14d,
        sp_c_af.units_sold_clicks_30d,
        sp_c_af.purchases_1d,
        sp_c_af.purchases_7d,
        sp_c_af.purchases_14d,
        sp_c_af.purchases_30d,
        sp_c_af.click_through_rate,
        sp_c_af.top_of_search_impression_share,
        sp_c_af.tenant_id,
        sp_c_af.campaign_budget_amount_usd,
        sp_c_af.cost_usd,
        sp_c_af.sales_1d_usd,
        sp_c_af.sales_7d_usd,
        sp_c_af.sales_14d_usd,
        sp_c_af.sales_30d_usd,
        sp_c_af.cost_per_click_usd,
        sp_c_af.conversion_rate,
        sp_c_af.target_product,
        sp_c_af.product_description,
        sp_c_af.product_code,
        sp_c_af.product_group,
        sp_c_af.product_color,
        u_bb_pc.portfolio_code,

        COALESCE(u_bb_pc.parent_code, sp_c_af.parent_code) as parent_code

    from sp_campaigns_with_added_fields as sp_c_af

    left join unique_bb_product_codes as u_bb_pc
        on sp_c_af.product_code = u_bb_pc.product_code

)

select * from get_sp_product_codes
