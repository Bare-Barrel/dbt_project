-- stg_exchangerate_host_api__exchange_rates.sql

with

exchange_rates as (

    select * from {{ source('exchangerate_host', 'exchange_rates') }}

),

convert_datetime_to_date as (

    select
        DATE(recorded_at) as recorded_at,
        base,
        target,
        rate
    from exchange_rates

)

select * from convert_datetime_to_date
