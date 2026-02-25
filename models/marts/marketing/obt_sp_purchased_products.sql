-- obt_sp_purchased_products.sql    TODO: remove fields that are not needed

with

sp_purchased_product as (

    select * from {{ ref('stg_sponsored_products__purchased_product') }}

)

select * from sp_purchased_product
