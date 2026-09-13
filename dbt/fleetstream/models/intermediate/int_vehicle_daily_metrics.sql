{{ config(materialized='view', tags=['intermediate', 'daily']) }}

-- Vehicle x day aggregates computed straight from event grain.
--
-- Deliberately NOT built by summing int_trip_metrics. A trip can start before
-- midnight and end after it, so attributing its distance to a single day would put
-- kilometres on the wrong date. Aggregating events by their own event_date assigns
-- every reading to the day it actually happened.

with telemetry as (

    select * from {{ ref('stg_telemetry') }}
    where {{ incremental_window('event_time') }}

),

aggregated as (

    select
        vehicle_id,
        event_date,

        count(*)                                    as event_count,
        count(distinct trip_id)                     as trip_count,
        count(distinct driver_id)                   as driver_count,

        -- Odometer delta within the day. Correct across trips and across the
        -- vehicle sitting idle, which a sum of per-trip distances is not.
        max(odometer_km) - min(odometer_km)         as distance_km,
        max(odometer_km)                            as odometer_end_km,

        avg(speed_kmh)                              as avg_speed_kmh,
        max(speed_kmh)                              as max_speed_kmh,

        max(fuel_level_pct)                         as fuel_level_max_pct,
        min(fuel_level_pct)                         as fuel_level_min_pct,

        avg(engine_temperature_c)                   as avg_engine_temperature_c,
        max(engine_temperature_c)                   as max_engine_temperature_c,
        min(battery_level_pct)                      as min_battery_level_pct,

        count_if(is_overspeed)                      as overspeed_event_count,
        count_if(is_overheating)                    as overheating_event_count,
        count_if(is_low_fuel)                       as low_fuel_event_count,

        -- Active minutes approximated from distinct reporting minutes. A parked
        -- vehicle reports nothing, so this is a usable proxy for utilisation.
        count(distinct date_trunc('minute', event_time)) as active_minutes,

        min(event_time)                             as first_event_at,
        max(event_time)                             as last_event_at

    from telemetry
    group by vehicle_id, event_date

)

select * from aggregated
