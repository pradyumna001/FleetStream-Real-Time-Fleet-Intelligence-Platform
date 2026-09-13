{{ config(materialized='view', tags=['staging', 'reference']) }}

select
    vehicle_id,
    vehicle_type,
    manufacturer,
    model,
    region,
    fleet_owner,
    max_speed_kmh,
    fuel_capacity_l,
    consumption_l_per_100km,
    overheat_threshold_c,
    has_faulty_cooling

from {{ source('silver', 'dim_vehicle') }}
