{{ config(materialized='view', tags=['staging', 'reference']) }}

select
    route_id,
    route_name,
    distance_km          as planned_distance_km,
    waypoint_count,
    origin_latitude,
    origin_longitude,
    destination_latitude,
    destination_longitude

from {{ source('silver', 'dim_route') }}
