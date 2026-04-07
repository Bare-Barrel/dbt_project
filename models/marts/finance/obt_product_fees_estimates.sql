-- obt_product_fees_estimates.sql

with

int_product_fees_estimates as (

    select * from {{ ref('int_add_fields_to_product_fees_estimates') }}

)

select * from int_product_fees_estimates
