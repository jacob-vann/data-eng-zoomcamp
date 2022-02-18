{{ config(materialized='view') }}

select * from {{ source('staging', 'fhv_tripdata') }}


