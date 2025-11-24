-- int_calculate_fields_for_sd_campaigns.sql

{{ config(materialized='view') }}

with

sd_campaign_usd as (

    select * from {{ ref('int_convert_sd_campaign_amounts_to_usd') }}

),

calculate_fields as (

    select
        date,
        created_at,
        updated_at,
        campaign_id,
        campaign_name,
        campaign_status,
        marketplace,
        impressions,
        clicks,
        units_sold_clicks,
        new_to_brand_units_sold_clicks,
        purchases_clicks,
        tenant_id,
        campaign_budget_amount_usd,
        cost_usd,
        sales_clicks_usd,
        new_to_brand_sales_clicks_usd,

        -- Cost per Click
        SAFE_DIVIDE(cost_usd, clicks) as cost_per_click,

        -- Click-through Rate
        SAFE_DIVIDE(clicks, impressions) as click_through_rate,

        -- Conversion Rate
        SAFE_DIVIDE(purchases_clicks, clicks) as conversion_rate,

        {# TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(3)]) as asin_string1,
        TRIM(SPLIT(campaign_name, "_")[SAFE_OFFSET(4)]) as asin_string2, #}

        -- Product Group TODO: join with master data to get color and pack size
        case
            when tenant_id = 2
                then
                    case
                        when
                            REGEXP_CONTAINS(campaign_name, r".*_B07YJ72D7M_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B086MZ1XB5_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B082WNSB4Y_.*")
                            then "GRIP SOCKS"
                        when
                            REGEXP_CONTAINS(campaign_name, r".*_B07ZHJB1TK_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HFPJG8_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HG38CH_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZHJHQCF_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZHHVSY6_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZH7R17H_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HFS6JL_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZHJFK6H_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HF6FRH_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HF3HBT_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZHJ5ZWT_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07R3FL6T1_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZHJHQCK_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HG731L_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZH7387X_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZHJKGT1_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07R3CX5Y5_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B081HFVFMM_.*")
                            then "CALF SLEEVES"
                        when
                            REGEXP_CONTAINS(campaign_name, r".*_B07TGK6SRL_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07Q1F1SM7_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07PZ95Y56_.*")
                            then "COMPRESSION SOCKS"
                        when REGEXP_CONTAINS(campaign_name, r".*_B07VQZN7LR_.*")
                            then "PF SOCKS"
                        when
                            REGEXP_CONTAINS(campaign_name, r".*_B07GH6GY5R_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B0845P7YVM_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B085KM9RPT_.*")
                            or REGEXP_CONTAINS(campaign_name, r".*_B07ZKNS35L_.*")
                            then "KNEE SLEEVES"
                    end
        end as product_group

    from sd_campaign_usd

)

select * from calculate_fields
