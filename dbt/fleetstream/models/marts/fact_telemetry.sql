{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        partition_by=['event_date'],
        tags=['mart', 'fact']
    )
}}

-- GRAIN: one row per telemetry reading.
--
-- Merged on event_id rather than appended. The incremental window reprocesses the
-- last few days on every run so late arrivals are picked up; without a merge on a
-- deterministic key, each run would re-insert everything inside that window and the
-- table would grow by a copy of the window every time it ran.

select
    event_id,
    vehicle_id,
    driver_id,
    trip_id,
    route_id,

    event_time,
    event_date,
    ingestion_time,
    ingestion_lag_seconds,

    latitude,
    longitude,
    speed_kmh,
    fuel_level_pct,
    engine_temperature_c,
    battery_level_pct,
    odometer_km,

    is_overspeed,
    is_overheating,
    is_low_fuel,

    kafka_partition,
    schema_version

from {{ ref('stg_telemetry') }}
where {{ incremental_window('event_time') }}
