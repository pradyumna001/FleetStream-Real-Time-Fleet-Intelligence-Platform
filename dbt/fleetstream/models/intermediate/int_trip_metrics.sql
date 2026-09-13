{{
    config(
        materialized='view',
        tags=['intermediate', 'trip']
    )
}}

-- Collapse event-grain telemetry to one row per trip.
--
-- This is the model that changes grain, and therefore the one where fanout gets
-- introduced if anything goes wrong. Everything downstream that needs trip-level
-- numbers must join to this rather than re-aggregating raw telemetry, so that trip
-- measures cannot be multiplied by the number of readings in the trip.
--
-- Distance is derived two independent ways - the odometer delta and the sum of
-- great-circle hops between consecutive GPS fixes - because a single measure has to
-- be trusted, whereas two that disagree reveal a bad sensor or dropped readings.

with telemetry as (

    select * from {{ ref('stg_telemetry') }}

),

with_previous_position as (

    select
        *,
        lag(latitude)  over (partition by trip_id order by event_time)  as prev_latitude,
        lag(longitude) over (partition by trip_id order by event_time)  as prev_longitude
    from telemetry

),

hops as (

    select
        *,
        case
            when prev_latitude is null then 0.0
            else {{ haversine_km('prev_latitude', 'prev_longitude', 'latitude', 'longitude') }}
        end as hop_km
    from with_previous_position

),

aggregated as (

    select
        trip_id,

        -- A trip belongs to one vehicle and one driver. min() is an arbitrary but
        -- deterministic pick that also makes a violation visible: if a trip ever
        -- spanned two vehicles, the uniqueness test on trip_id would still pass
        -- while distinct_vehicles below would flag it.
        min(vehicle_id)                                  as vehicle_id,
        min(driver_id)                                   as driver_id,
        min(route_id)                                    as route_id,
        count(distinct vehicle_id)                       as distinct_vehicles,

        min(event_time)                                  as trip_started_at,
        max(event_time)                                  as trip_ended_at,
        cast(min(event_time) as date)                    as trip_start_date,
        date_diff('second', min(event_time), max(event_time)) as duration_seconds,

        count(*)                                         as event_count,

        -- distance, two ways
        max(odometer_km) - min(odometer_km)              as odometer_distance_km,
        sum(hop_km)                                      as gps_distance_km,

        avg(speed_kmh)                                   as avg_speed_kmh,
        max(speed_kmh)                                   as max_speed_kmh,

        -- Fuel is a percentage of a vehicle-specific tank, so it is converted to
        -- litres in the mart where dim_vehicle's capacity is available. Keeping the
        -- percentages here avoids a join that would change this model's grain risk.
        max(fuel_level_pct)                              as fuel_level_start_pct,
        min(fuel_level_pct)                              as fuel_level_end_pct,

        max(engine_temperature_c)                        as max_engine_temperature_c,
        avg(engine_temperature_c)                        as avg_engine_temperature_c,
        min(battery_level_pct)                           as min_battery_level_pct,

        count_if(is_overspeed)                           as overspeed_event_count,
        count_if(is_overheating)                         as overheating_event_count,
        count_if(is_low_fuel)                            as low_fuel_event_count,

        max(ingestion_lag_seconds)                       as max_ingestion_lag_seconds

    from hops
    where {{ incremental_window('event_time') }}
    group by trip_id

)

select
    *,

    -- A trip whose last reading is recent is probably still running. Marking those
    -- incomplete keeps in-flight trips out of averages, where a trip that is two
    -- minutes old would otherwise drag the mean duration down.
    trip_ended_at < current_timestamp - interval '30' minute as is_complete,

    -- Enough readings to compute anything meaningful from.
    event_count >= {{ var('min_events_per_trip') }}         as has_sufficient_events,

    case
        when duration_seconds > 0
            then odometer_distance_km / (duration_seconds / 3600.0)
        else null
    end                                                     as implied_avg_speed_kmh

from aggregated
