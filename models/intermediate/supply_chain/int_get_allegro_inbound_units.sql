-- int_get_allegro_inbound_units.sql

{{ config(materialized='view') }}

with

matched_mintsoft_inventory as (

    select * from {{ ref('int_match_tsp_old_skus_to_current') }}

),

rymora_allegro_product_inventory as (

    select * from {{ ref('stg_mintsoft_api__rymora_allegro_product_inventory') }}

),

get_inbound_units as (

    select
        m_mint.account_name,
        m_mint.warehouse_name,
        m_mint.breakdown,
        m_mint.warehouse_id,
        m_mint.client_id,
        m_mint.product_id,
        m_mint.stock_level,
        m_mint.total_stock_level,
        m_mint.low_stock_level,
        m_mint.preorderable,
        m_mint.bundle,
        m_mint.recorded_at,
        m_mint.recorded_date,
        m_mint.last_updated,
        m_mint.sku,

        a_p_i.on_order

    from matched_mintsoft_inventory as m_mint

    left join rymora_allegro_product_inventory as a_p_i
        on
            m_mint.account_name = a_p_i.account_name
            and m_mint.warehouse_id = a_p_i.warehouse_id
            and m_mint.product_id = a_p_i.product_id
            and m_mint.sku = a_p_i.sku
            and m_mint.recorded_date = a_p_i.recorded_date

)

select * from get_inbound_units
