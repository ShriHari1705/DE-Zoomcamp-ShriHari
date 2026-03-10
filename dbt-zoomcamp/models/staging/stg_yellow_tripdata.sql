{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'yellow_tripdata_2019') }}
    union all
    select * from {{ source('staging', 'yellow_tripdata_2020') }}
    union all
    select * from {{ source('staging', 'yellow_tripdata_2021') }}
),

renamed as (
    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as tripid,
        cast(VendorID as integer)       as vendorid,
        cast(RatecodeID as integer)     as ratecodeid,
        cast(PULocationID as integer)   as pickup_locationid,
        cast(DOLocationID as integer)   as dropoff_locationid,

        -- timestamps
        cast(tpep_pickup_datetime as timestamp)  as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        store_and_fwd_flag,
        cast(passenger_count as integer)  as passenger_count,
        cast(trip_distance as numeric)    as trip_distance,
        1                                 as trip_type,  -- yellow taxis are always street-hail

        -- payment info
        cast(fare_amount as numeric)            as fare_amount,
        cast(extra as numeric)                  as extra,
        cast(mta_tax as numeric)                as mta_tax,
        cast(tip_amount as numeric)             as tip_amount,
        cast(tolls_amount as numeric)           as tolls_amount,
        cast(null as numeric)                   as ehail_fee,
        cast(improvement_surcharge as numeric)  as improvement_surcharge,
        cast(total_amount as numeric)           as total_amount,
        cast(payment_type as integer)           as payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description,
        cast(congestion_surcharge as numeric)   as congestion_surcharge

    from source
    where tpep_pickup_datetime >= '2019-01-01'
)

select * from renamed
