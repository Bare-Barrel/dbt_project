-- dim_campaigns.sql SCD type 2

with

sp_campaigns as (

    select
        campaign_id,
        date as record_date,
        campaign_name,
        campaign_status

    from {{ source('sponsored_products', 'campaign') }}

),

sb_campaigns as (

    select
        campaign_id,
        date as record_date,
        campaign_name,
        campaign_status

    from {{ source('sponsored_brands', 'campaign') }}

),

sd_campaigns as (

    select
        campaign_id,
        date as record_date,
        campaign_name,
        campaign_status

    from {{ source('sponsored_display', 'campaign') }}

),

union_all_campaigns as (

    select * from sp_campaigns
    union all
    select * from sb_campaigns
    union all
    select * from sd_campaigns

),

-- 1. Identify "change groups"
--    If any attribute changes vs the previous day for the same campaign,
--    start a new SCD2 row.
campaign_changes as (

    select
        *,
        LAG(campaign_name) over (partition by campaign_id order by record_date) as prev_name,
        LAG(campaign_status) over (partition by campaign_id order by record_date) as prev_status

    from union_all_campaigns

),

-- 2. Determine whether each row starts a new version
version_flags as (

    select
        *,
        case
            when prev_name is null
                then 1
            when
                campaign_name != prev_name
                or campaign_status != prev_status
                then 1
            else 0
        end as is_new_version_flag

    from campaign_changes

),

-- 3. Define version groups using a cumulative sum
version_groups as (

    select
        *,
        SUM(is_new_version_flag)
            over (partition by campaign_id order by record_date) as version_group

    from version_flags

),

-- 4. Produce SCD2 rows → start_date / end_date per group
scd2 as (

    select
        campaign_id,
        campaign_name,
        campaign_status,
        MIN(record_date) as start_date,
        MAX(record_date) as end_date

    from version_groups

    group by
        campaign_id,
        campaign_name,
        campaign_status,
        version_group

),

-- 5. Add is_current and surrogate_key
add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['campaign_id', 'start_date']) }} as campaign_sk,
        campaign_id,
        campaign_name,
        campaign_status,
        start_date,
        end_date,
        end_date = (
            select MAX(uac.record_date)
            from union_all_campaigns as uac
            where uac.campaign_id = scd2.campaign_id
        ) as is_current

    from scd2

)

select * from add_surrogate_key
