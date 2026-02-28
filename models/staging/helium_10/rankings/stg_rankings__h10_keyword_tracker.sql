-- stg_rankings__h10_keyword_tracker.sql

with

h10_keyword_tracker as (

    select * from {{ source('rankings','h10_keyword_tracker') }}

),

rename_fields as (

    select
        date_added,
        created_at,
        updated_at,
        asin,
        title,
        keyword,
        marketplace as marketplace_url,
        marketplace_id as marketplace,
        organic_rank,
        search_volume,
        sponsored_position

    from h10_keyword_tracker

)

select * from rename_fields
