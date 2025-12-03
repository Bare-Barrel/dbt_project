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

    from {{ ref('dim_products') }}

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
        sp_c_af.product_code,
        sp_c_af.product_color,
        u_bb_pc.parent_code,

        -- Portfolio Code
        case
            when sp_c_af.tenant_id = 1
                then COALESCE(u_bb_pc.portfolio_code, sp_c_af.portfolio_name)
            when sp_c_af.tenant_id = 2
                then u_bb_pc.portfolio_code
        end as portfolio_code

    from sp_campaigns_with_added_fields as sp_c_af

    left join unique_bb_product_codes as u_bb_pc
        on sp_c_af.product_code = u_bb_pc.product_code

),

get_sp_parent_pack as (

    select
        date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        portfolio_id,
        portfolio_name,
        marketplace,
        impressions,
        clicks,
        units_sold_clicks_1d,
        units_sold_clicks_7d,
        units_sold_clicks_14d,
        units_sold_clicks_30d,
        purchases_1d,
        purchases_7d,
        purchases_14d,
        purchases_30d,
        click_through_rate,
        top_of_search_impression_share,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_1d_usd,
        sales_7d_usd,
        sales_14d_usd,
        sales_30d_usd,
        cost_per_click_usd,
        conversion_rate,
        target_product,
        product_code,
        product_color,
        portfolio_code,

        -- Parent Code
        case
            when tenant_id = 1
                then COALESCE(parent_code, TRIM(SPLIT(portfolio_code, "-")[SAFE_OFFSET(0)]))
            when tenant_id = 2
                then
                    case
                        when
                            REGEXP_CONTAINS(campaign_name, r"^ElbSlv")
                            or REGEXP_CONTAINS(campaign_name, r"^ElbowSleeve")
                            then "R_ELBO-SLE"
                        when
                            REGEXP_CONTAINS(campaign_name, r"^Comp Sleeve-C")
                            or REGEXP_CONTAINS(campaign_name, r"^CompSock-C")
                            or REGEXP_CONTAINS(campaign_name, r"^CmpSk-C")
                            then "R_COMP-SOCKS-CU"
                        when
                            REGEXP_CONTAINS(campaign_name, r"^Comp Sleeve-P")
                            or REGEXP_CONTAINS(campaign_name, r"^CompSock-P")
                            or REGEXP_CONTAINS(campaign_name, r"^CmpSk-P")
                            then "R_COMP-SOCKS-PL"
                        when
                            REGEXP_CONTAINS(campaign_name, r"^KneeSleeve")
                            or REGEXP_CONTAINS(campaign_name, r"^KnSlv")
                            then "R_KNEE-SLE"
                        when
                            REGEXP_CONTAINS(campaign_name, r"^HikSk")
                            or REGEXP_CONTAINS(campaign_name, r"(?i)^wool socks")
                            or REGEXP_CONTAINS(campaign_name, r"^Ankle Sleeve")
                            then "R_HIKE-SOC"
                        when REGEXP_CONTAINS(campaign_name, r"^elbow/knee sleeves")
                            then "R_ELBO-SLE/R_KNEE-SLE"
                        when
                            REGEXP_CONTAINS(campaign_name, r"^Calf C Socks")
                            or REGEXP_CONTAINS(campaign_name, r"^Calf C Sleeves")
                            or REGEXP_CONTAINS(campaign_name, r"^ClfSlv")
                            or REGEXP_CONTAINS(campaign_name, r"^calf compression sleeves")
                            then "R_CALF-SLEEV"
                        when
                            REGEXP_CONTAINS(campaign_name, r"^PfSk")
                            or REGEXP_CONTAINS(campaign_name, r"^Plantar Socks")
                            then "R_PF-SOCKS"
                        when
                            REGEXP_CONTAINS(campaign_name, r"^GrpSk")
                            or REGEXP_CONTAINS(campaign_name, r"(?i)^nonslip")
                            or REGEXP_CONTAINS(campaign_name, r"(?i)^non-slip socks")
                            then "R_GRIP-SOCKS"
                        when
                            REGEXP_CONTAINS(campaign_name, r"(?i)^All.*")
                            or REGEXP_CONTAINS(campaign_name, r"(?i)^Ivy-All.*")
                            then "OTHER"
                    end
        end as parent_code,

        -- Product Pack Size
        case
            when tenant_id = 2
                then
                    case
                        when
                            (
                                REGEXP_CONTAINS(target_product, r"^ClfSlv")
                                or REGEXP_CONTAINS(target_product, r"^PfSk")
                                or REGEXP_CONTAINS(target_product, r"^HikSk")
                                or REGEXP_CONTAINS(target_product, r"^GrpSk")
                            )
                            then
                                case
                                    when REGEXP_CONTAINS(target_product, r"A1")
                                        then "1PR"
                                    when REGEXP_CONTAINS(target_product, r"A2")
                                        then "2PR"
                                    when REGEXP_CONTAINS(target_product, r"A3")
                                        then "3PR"
                                end
                        when REGEXP_CONTAINS(target_product, r"^ElbSlv")
                            then
                                case
                                    when REGEXP_CONTAINS(target_product, r"A1")
                                        then "1PC"
                                    when REGEXP_CONTAINS(target_product, r"A2")
                                        then "2PC"
                                end
                        when REGEXP_CONTAINS(target_product, r"^KnSlv")
                            then
                                case
                                    when REGEXP_CONTAINS(target_product, r"B1")
                                        then "1PC"
                                    when REGEXP_CONTAINS(target_product, r"B2")
                                        then "2PC"
                                end
                        when
                            REGEXP_CONTAINS(target_product, r"^CmpSk")
                            and (
                                REGEXP_CONTAINS(target_product, r"C1")
                                or REGEXP_CONTAINS(target_product, r"P1")
                            )
                            then "1PR"
                    end
        end as product_pack_size

    from get_sp_product_codes

)

select * from get_sp_parent_pack
