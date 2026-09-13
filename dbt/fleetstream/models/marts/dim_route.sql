{{ config(materialized='table', tags=['mart', 'dimension']) }}

select
    route_id,
    route_name,
    planned_distance_km,
    waypoint_count,
    origin_latitude,
    origin_longitude,
    destination_latitude,
    destination_longitude,

    case
        when planned_distance_km < 50  then 'short_haul'
        when planned_distance_km < 250 then 'medium_haul'
        else 'long_haul'
    end as haul_type

from {{ ref('stg_routes') }}
