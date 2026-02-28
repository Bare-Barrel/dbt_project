-- obt_h10_keyword_tracker.sql

with

h10_keyword_tracker as (

    select * from {{ ref('stg_rankings__h10_keyword_tracker') }}

),

remove_marketplace_url as (

    select
        asin,
        title,
        keyword,
        marketplace,
        organic_rank,
        search_volume,
        sponsored_position,
        date_added,
        created_at,
        updated_at

    from h10_keyword_tracker

),

replace_306_with_null as (

    select
        asin,
        title,
        keyword,
        marketplace,
        search_volume,
        sponsored_position,
        date_added,
        created_at,
        updated_at,

        -- Remove 306 in organic_rank
        case
            when organic_rank = 306
                then CAST(null as integer)
            else organic_rank
        end as organic_rank

    from remove_marketplace_url

)

select * from replace_306_with_null
