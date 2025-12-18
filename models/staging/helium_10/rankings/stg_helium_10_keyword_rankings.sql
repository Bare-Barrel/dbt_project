-- stg_helium_10_keyword_rankings.sql

with

source as (

    select * from {{ source('rankings','h10_keyword_tracker') }}

),

remove_airbyte_fields as (

    select
        asin,
        title,
        keyword,
        created_at,
        date_added,
        updated_at,
        marketplace as marketplace_url,
        organic_rank,
        search_volume,
        marketplace_id as marketplace,
        sponsored_position

    from source

)

select * from remove_airbyte_fields
