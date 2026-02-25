-- obt_sp_advertised_products.sql   TODO: remove fields that are not needed

with

sp_advertised_product as (

    select * from {{ ref('stg_sponsored_products__advertised_product') }}

)

select * from sp_advertised_product
