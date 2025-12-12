-- dim_ppc_products.sql

with

campaigns as (

    select * from {{ ref('int_union_campaigns') }}

),

campaign_placements as (

    select * from {{ ref('int_union_campaign_placements') }}

),

select_campaign_fields as (

    select
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,
        tenant_id

    from campaigns

),

get_unique_campaign_product_values as (

    select distinct *
    from select_campaign_fields

),

select_placement_fields as (

    select
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,
        tenant_id

    from campaign_placements

),

get_unique_placement_product_values as (

    select distinct *
    from select_placement_fields

),

union_campaign_and_placement_product_codes as (

    select * from get_unique_campaign_product_values

    union all

    select * from get_unique_placement_product_values

),

get_distinct_rows as (

    select distinct *
    from union_campaign_and_placement_product_codes

),

add_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['parent_code', 'portfolio_code', 'product_code', 'product_color', 'product_pack_size', 'tenant_id']) }} as ppc_product_sk,
        parent_code,
        portfolio_code,
        product_code,
        product_color,
        product_pack_size,
        tenant_id

    from get_distinct_rows

)

select * from add_surrogate_key
