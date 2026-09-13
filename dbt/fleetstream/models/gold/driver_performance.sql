{{ config(materialized='view', tags=['gold', 'performance']) }}

-- Per-driver behaviour, built at trip grain.
--
-- Trip grain rather than event grain matters here: a driver who happens to run long
-- routes accumulates more readings, so counting raw events would rank them as worse
-- purely for driving further. The rates below are normalised per 100 km so drivers
-- are actually comparable.

with trips as (

    select * from {{ ref('fact_trip') }}
    -- In-flight trips have partial distance and duration; including them would
    -- understate averages for whoever happens to be driving right now.
    where is_complete

),

drivers as (

    select * from {{ ref('dim_driver') }}

),

aggregated as (

    select
        driver_id,
        count(*)                                        as total_trips,
        sum(distance_km)                                as total_distance_km,
        sum(duration_seconds) / 3600.0                  as total_driving_hours,
        avg(avg_speed_kmh)                              as avg_speed_kmh,
        max(max_speed_kmh)                              as max_speed_kmh,
        sum(overspeed_event_count)                      as overspeed_event_count,
        sum(overheating_event_count)                    as overheating_event_count,
        sum(fuel_consumed_l)                            as total_fuel_consumed_l,
        avg(duration_seconds) / 60.0                    as avg_trip_minutes
    from trips
    group by driver_id

)

select
    d.driver_id,
    d.driver_name,
    d.driver_category,
    d.license_type,
    d.risk_band,

    a.total_trips,
    round(a.total_distance_km, 2)                       as total_distance_km,
    round(a.total_driving_hours, 2)                     as total_driving_hours,
    round(a.avg_trip_minutes, 1)                        as avg_trip_minutes,
    round(a.avg_speed_kmh, 2)                           as avg_speed_kmh,
    round(a.max_speed_kmh, 2)                           as max_speed_kmh,

    a.overspeed_event_count,
    a.overheating_event_count,

    case
        when a.total_distance_km > 0
            then round(a.overspeed_event_count / a.total_distance_km * 100, 3)
        else null
    end                                                 as overspeed_events_per_100km,

    case
        when a.total_distance_km > 0
            then round(a.total_fuel_consumed_l / a.total_distance_km * 100, 2)
        else null
    end                                                 as l_per_100km

from aggregated a
join drivers d on a.driver_id = d.driver_id
