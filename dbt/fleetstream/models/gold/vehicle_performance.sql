{{ config(materialized='view', tags=['gold', 'performance']) }}

-- Per-vehicle rollup for the fleet-health table and the maintenance shortlist.

with daily as (

    select * from {{ ref('fact_vehicle_daily') }}

),

vehicles as (

    select * from {{ ref('dim_vehicle') }}

),

aggregated as (

    select
        vehicle_id,
        count(distinct event_date)                      as active_days,
        sum(trip_count)                                 as total_trips,
        sum(total_distance_km)                          as total_distance_km,
        sum(total_fuel_consumed_l)                      as total_fuel_consumed_l,
        avg(avg_speed_kmh)                              as avg_speed_kmh,
        max(max_speed_kmh)                              as max_speed_kmh,
        max(max_engine_temperature_c)                   as max_engine_temperature_c,
        avg(avg_engine_temperature_c)                   as avg_engine_temperature_c,
        sum(engine_overheat_count)                      as overheat_event_count,
        sum(overspeed_event_count)                      as overspeed_event_count,
        sum(incident_count)                             as total_incidents,
        avg(utilisation_pct)                            as avg_utilisation_pct,
        max(event_date)                                 as last_active_date
    from daily
    group by vehicle_id

)

select
    v.vehicle_id,
    v.vehicle_description,
    v.vehicle_type,
    v.region,
    v.fleet_owner,
    v.consumption_band,
    v.has_faulty_cooling,

    a.active_days,
    a.total_trips,
    round(a.total_distance_km, 2)                       as total_distance_km,
    round(a.total_fuel_consumed_l, 2)                   as total_fuel_consumed_l,

    case
        when a.total_distance_km > 0
            then round(a.total_fuel_consumed_l / a.total_distance_km * 100, 2)
        else null
    end                                                 as actual_l_per_100km,
    v.consumption_l_per_100km                           as rated_l_per_100km,

    -- Burning materially more than rated is a maintenance signal in its own right,
    -- and one that shows up long before a temperature threshold is breached.
    case
        when a.total_distance_km > 0 and v.consumption_l_per_100km > 0
            then round(
                (a.total_fuel_consumed_l / a.total_distance_km * 100)
                / v.consumption_l_per_100km, 3)
        else null
    end                                                 as consumption_vs_rated_ratio,

    round(a.avg_speed_kmh, 2)                           as avg_speed_kmh,
    round(a.max_speed_kmh, 2)                           as max_speed_kmh,
    round(a.max_engine_temperature_c, 2)                as max_engine_temperature_c,
    round(a.avg_engine_temperature_c, 2)                as avg_engine_temperature_c,

    a.overheat_event_count,
    a.overspeed_event_count,
    a.total_incidents,
    round(a.avg_utilisation_pct, 2)                     as avg_utilisation_pct,
    a.last_active_date,

    -- A rule-based maintenance flag rather than a score. An operator has to be able
    -- to see exactly why a vehicle was flagged before sending it to a workshop.
    (a.overheat_event_count > 0
        or a.max_engine_temperature_c > {{ var('critical_overheat_threshold_c') }})
                                                        as needs_maintenance_review

from aggregated a
join vehicles v on a.vehicle_id = v.vehicle_id
