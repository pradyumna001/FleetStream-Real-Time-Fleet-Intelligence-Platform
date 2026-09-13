{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trip_id',
        partition_by=['trip_start_date'],
        tags=['mart', 'fact']
    )
}}

-- GRAIN: one row per trip.
--
-- Note this is a DIFFERENT grain from fact_telemetry, which holds roughly a
-- thousand rows for every one row here. Joining the two and summing a trip measure
-- would multiply it by the trip's reading count - the classic fanout error. Any
-- model needing both must aggregate telemetry to trip grain first.

with trips as (

    select * from {{ ref('int_trip_metrics') }}

),

vehicles as (

    select * from {{ ref('stg_vehicles') }}

)

select
    t.trip_id,
    t.vehicle_id,
    t.driver_id,
    t.route_id,

    t.trip_started_at,
    t.trip_ended_at,
    t.trip_start_date,
    t.duration_seconds,
    t.duration_seconds / 60.0                       as duration_minutes,

    t.event_count,
    t.is_complete,
    t.has_sufficient_events,

    t.odometer_distance_km                          as distance_km,
    t.gps_distance_km,

    -- The two independent distance measures should agree closely. A large gap means
    -- dropped readings or a faulty odometer, so it is surfaced rather than hidden.
    abs(t.odometer_distance_km - t.gps_distance_km) as distance_discrepancy_km,

    t.avg_speed_kmh,
    t.max_speed_kmh,
    t.implied_avg_speed_kmh,

    t.fuel_level_start_pct,
    t.fuel_level_end_pct,
    t.fuel_level_start_pct - t.fuel_level_end_pct   as fuel_consumed_pct,

    -- Percentage converted to litres using this vehicle's tank. Joining to the
    -- vehicle dimension here is safe: it is one row per vehicle, so it cannot
    -- change the grain.
    (t.fuel_level_start_pct - t.fuel_level_end_pct) / 100.0 * v.fuel_capacity_l
                                                    as fuel_consumed_l,

    case
        when t.odometer_distance_km > 0
            then ((t.fuel_level_start_pct - t.fuel_level_end_pct) / 100.0 * v.fuel_capacity_l)
                 / t.odometer_distance_km * 100
        else null
    end                                             as actual_l_per_100km,

    v.consumption_l_per_100km                       as rated_l_per_100km,

    t.max_engine_temperature_c,
    t.avg_engine_temperature_c,
    t.min_battery_level_pct,

    t.overspeed_event_count,
    t.overheating_event_count,
    t.low_fuel_event_count,
    t.max_ingestion_lag_seconds,

    t.distinct_vehicles

from trips t
left join vehicles v on t.vehicle_id = v.vehicle_id
where {{ incremental_window('t.trip_started_at') }}
