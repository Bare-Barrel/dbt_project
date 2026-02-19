-- int_match_tsp_old_skus_to_current.sql

{{ config(materialized='view') }}

with

mintsoft_wh_stock_levels as (

    select * from {{ ref('stg_mintsoft_api__rymora_warehouse_stock_levels') }}

),

rymora_product_md as (

    select * from {{ ref('stg_google_sheets__rymora_product_md') }}

),

match_tsp_prev_skus_to_current as (

    select
        mint.account_name,
        mint.warehouse_name,
        mint.sku,
        mint.breakdown,
        mint.warehouse_id,
        mint.client_id,
        mint.product_id,
        mint.stock_level,
        mint.total_stock_level,
        mint.low_stock_level,
        mint.preorderable,
        mint.bundle,
        mint.recorded_at,
        mint.recorded_date,
        mint.last_updated,

        case
            when
                mint.account_name = "TSP"
                and REGEXP_CONTAINS(mint.sku, r"^RMA-SPO-")
                then ry_p_md.current_sku
        end as current_sku

    from mintsoft_wh_stock_levels as mint

    left join rymora_product_md as ry_p_md
        on mint.sku = ry_p_md.previous_sku

),

match_tsp_old_skus_to_current as (

    select
        m_prev.account_name,
        m_prev.warehouse_name,
        m_prev.sku,
        m_prev.breakdown,
        m_prev.warehouse_id,
        m_prev.client_id,
        m_prev.product_id,
        m_prev.stock_level,
        m_prev.total_stock_level,
        m_prev.low_stock_level,
        m_prev.preorderable,
        m_prev.bundle,
        m_prev.recorded_at,
        m_prev.recorded_date,
        m_prev.last_updated,

        case
            when
                m_prev.account_name = "TSP"
                and REGEXP_CONTAINS(m_prev.sku, r"^RMA-SPO-")
                and m_prev.current_sku is null
                then ry_p_md.current_sku
            else m_prev.current_sku
        end as current_sku

    from match_tsp_prev_skus_to_current as m_prev

    left join rymora_product_md as ry_p_md
        on m_prev.sku = ry_p_md.old_sku

),

transfer_tsp_current_sku_to_sku_field as (

    select
        account_name,
        warehouse_name,
        breakdown,
        warehouse_id,
        client_id,
        product_id,
        stock_level,
        total_stock_level,
        low_stock_level,
        preorderable,
        bundle,
        recorded_at,
        recorded_date,
        last_updated,

        case
            when
                account_name = "TSP"
                and REGEXP_CONTAINS(sku, r"^RMA-SPO-")
                and current_sku is not null
                then current_sku
            else sku
        end as sku

    from match_tsp_old_skus_to_current

)

select * from transfer_tsp_current_sku_to_sku_field
