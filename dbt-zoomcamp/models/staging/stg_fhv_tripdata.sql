{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'fhv_tripdata_2019') }}
),

renamed as (
    select
        dispatching_base_num,
        cast(pickup_datetime as timestamp)   as pickup_datetime,
        cast(dropOff_datetime as timestamp)  as dropoff_datetime,
        cast(PUlocationID as integer)        as pickup_locationid,
        cast(DOlocationID as integer)        as dropoff_locationid,
        SR_Flag                              as sr_flag,
        Affiliated_base_number               as affiliated_base_number

    from source
    where pickup_datetime >= '2019-01-01'
)

select * from renamed
