{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='incident_id',
        partition_by=['event_date'],
        tags=['mart', 'fact']
    )
}}

-- GRAIN: one row per detected incident (one reading that breached one threshold).

select
    incident_id,
    event_id,
    vehicle_id,
    driver_id,
    trip_id,
    route_id,

    event_time,
    event_date,

    incident_type,
    severity,

    latitude,
    longitude,
    speed_kmh,
    engine_temperature_c,
    fuel_level_pct,
    battery_level_pct,

    vehicle_type,
    region,
    driver_name,
    driver_category

from {{ ref('int_incidents') }}
where {{ incremental_window('event_time') }}
