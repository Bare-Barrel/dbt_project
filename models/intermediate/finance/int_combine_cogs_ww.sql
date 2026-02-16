-- int_combine_cogs_ww.sql 02

{{ config(materialized='view') }}

with

pivoted_bb_cogs_ww as (

    select
        start_date,
        end_date,
        parent_product,
        product_code,
        sku,
        asin,
        marketplace,
        currency_code,
        cogs,
        fba_fee,
        storage_fee,
        returns_cost,
        tenant_id

    from {{ ref('int_pivot_bb_cogs_ww') }}

),

pivoted_rymora_cogs_ww as (

    select
        start_date,
        end_date,
        parent_product,
        product_code,
        sku,
        asin,
        marketplace,
        currency_code,
        cogs,
        fba_fee,
        storage_fee,
        returns_cost,
        tenant_id

    from {{ ref('int_pivot_rymora_cogs_ww') }}

),

union_all_cogs_ww as (

    select * from pivoted_bb_cogs_ww

    union all

    select * from pivoted_rymora_cogs_ww

)

{# expand_dates as (

    select
        est_fee_date,
        parent_product,
        product_code,
        sku,
        asin,
        marketplace,
        tenant_id,
        currency_code,
        cogs,
        fba_fee,
        storage_fee,
        returns_cost

    from union_all_cogs_ww,
        UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) as est_fee_date

) #}

select * from union_all_cogs_ww
