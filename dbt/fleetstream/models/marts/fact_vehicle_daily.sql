{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='vehicle_day_key',
        partition_by=['event_date'],
        tags=['mart', 'fact']
    )
}}

-- GRAIN: one row per vehicle per day.
--
-- The merge key is a surrogate over (vehicle_id, event_date) because the natural key
-- is composite and merge needs a single column. It is deterministic, so reprocessing
-- a day updates that day's rows in place - which is exactly what has to happen when
-- late-arriving events change yesterday's totals.

with daily as (

    select * from {{ ref('int_vehicle_daily_metrics') }}

),

vehicles as (

    select * from {{ ref('stg_vehicles') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['d.vehicle_id', 'd.event_date']) }} as vehicle_day_key,

    d.vehicle_id,
    d.event_date,
    d.event_date                                    as date_key,

    v.vehicle_type,
    v.region,
    v.fleet_owner,

    d.event_count,
    d.trip_count,
    d.driver_count,

    d.distance_km                                   as total_distance_km,
    d.odometer_end_km,

    d.avg_speed_kmh,
    d.max_speed_kmh,

    (d.fuel_level_max_pct - d.fuel_level_min_pct) / 100.0 * v.fuel_capacity_l
                                                    as total_fuel_consumed_l,

    d.avg_engine_temperature_c,
    d.max_engine_temperature_c,
    d.min_battery_level_pct,

    d.overspeed_event_count,
    d.overheating_event_count                       as engine_overheat_count,
    d.low_fuel_event_count,
    d.overspeed_event_count + d.overheating_event_count + d.low_fuel_event_count
                                                    as incident_count,

    d.active_minutes,
    -- Utilisation against a 24-hour day. Reporting minutes are the proxy for active
    -- time because a parked vehicle sends nothing.
    round(d.active_minutes / 1440.0 * 100, 2)       as utilisation_pct,

    d.first_event_at,
    d.last_event_at

from daily d
left join vehicles v on d.vehicle_id = v.vehicle_id
where {{ incremental_window('d.event_date') }}
