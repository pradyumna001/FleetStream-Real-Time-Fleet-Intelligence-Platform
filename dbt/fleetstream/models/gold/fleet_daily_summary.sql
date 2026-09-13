{{ config(materialized='view', tags=['gold', 'summary']) }}

-- Fleet-wide KPIs, one row per day. The top-level dashboard model.
--
-- Built from fact_vehicle_daily (vehicle x day) rather than recomputed from event
-- grain, so the fleet totals are guaranteed consistent with the per-vehicle numbers
-- a user sees when they drill in. Two independent aggregations of the same measure
-- eventually disagree, and the resulting "the dashboard is wrong" reports are
-- expensive to chase.

with daily as (

    select * from {{ ref('fact_vehicle_daily') }}

)

select
    event_date,

    count(distinct vehicle_id)                          as active_vehicles,
    sum(trip_count)                                     as total_trips,
    sum(event_count)                                    as total_events,

    round(sum(total_distance_km), 2)                    as total_distance_km,
    round(sum(total_fuel_consumed_l), 2)                as total_fuel_consumed_l,

    case
        when sum(total_distance_km) > 0
            then round(sum(total_fuel_consumed_l) / sum(total_distance_km) * 100, 2)
        else null
    end                                                 as fleet_l_per_100km,

    round(avg(avg_speed_kmh), 2)                        as avg_speed_kmh,
    round(max(max_speed_kmh), 2)                        as max_speed_kmh,

    sum(incident_count)                                 as total_incidents,
    sum(engine_overheat_count)                          as overheat_incidents,
    sum(overspeed_event_count)                          as overspeed_incidents,
    sum(low_fuel_event_count)                           as low_fuel_incidents,

    round(avg(utilisation_pct), 2)                      as avg_utilisation_pct,
    count_if(max_engine_temperature_c > {{ var('overheat_threshold_c') }})
                                                        as vehicles_overheating

from daily
group by event_date
