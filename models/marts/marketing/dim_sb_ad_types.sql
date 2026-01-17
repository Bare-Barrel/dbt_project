-- dim_sb_ad_types.sql

with

agg_campaign_placements as (

    select * from {{ ref('obt_aggregated_campaign_placements') }}

),

get_unique_sb_ad_type_values as (

    select distinct sb_ad_type
    from agg_campaign_placements

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['sb_ad_type']) }} as sb_ad_type_sk,
        sb_ad_type

    from get_unique_sb_ad_type_values

)

select * from add_surrogate_key
