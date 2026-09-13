{{ config(materialized='view', tags=['staging', 'reference']) }}

select
    driver_id,
    driver_name,
    driver_category,
    license_type,
    aggression_index

from {{ source('silver', 'dim_driver') }}
