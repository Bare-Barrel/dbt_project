-- obt_sd_advertised_products.sql   TODO: remove fields that are not needed

with

sd_advertised_product as (

    select * from {{ ref('stg_sponsored_display__advertised_product') }}

)

select * from sd_advertised_product
