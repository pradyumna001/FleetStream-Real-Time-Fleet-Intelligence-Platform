{{ config(materialized='view', tags=['gold', 'performance']) }}

-- Route-level comparison, for route optimisation and delay analysis.
--
-- The useful comparison is actual against planned: a route is not slow because it is
-- long, it is slow when it takes longer than its own distance implies. Comparing raw
-- durations across routes of different lengths would just rank them by distance.

with trips as (

    select * from {{ ref('fact_trip') }}
    where is_complete
      and route_id is not null

),

routes as (

    select * from {{ ref('dim_route') }}

),

aggregated as (

    select
        route_id,
        count(*)                                        as total_trips,
        count(distinct vehicle_id)                      as distinct_vehicles,
        count(distinct driver_id)                       as distinct_drivers,

        avg(duration_seconds) / 60.0                    as avg_duration_minutes,
        min(duration_seconds) / 60.0                    as min_duration_minutes,
        max(duration_seconds) / 60.0                    as max_duration_minutes,
        approx_percentile(duration_seconds, 0.5) / 60.0 as median_duration_minutes,
        approx_percentile(duration_seconds, 0.95) / 60.0 as p95_duration_minutes,

        avg(distance_km)                                as avg_distance_km,
        avg(avg_speed_kmh)                              as avg_speed_kmh,
        avg(fuel_consumed_l)                            as avg_fuel_consumed_l,
        sum(overspeed_event_count)                      as overspeed_event_count,
        sum(overheating_event_count)                    as overheating_event_count
    from trips
    group by route_id

)

select
    r.route_id,
    r.route_name,
    r.haul_type,
    r.planned_distance_km,

    a.total_trips,
    a.distinct_vehicles,
    a.distinct_drivers,

    round(a.avg_distance_km, 2)                         as avg_actual_distance_km,
    round(a.avg_distance_km - r.planned_distance_km, 2) as avg_distance_variance_km,

    round(a.avg_duration_minutes, 1)                    as avg_duration_minutes,
    round(a.median_duration_minutes, 1)                 as median_duration_minutes,
    round(a.p95_duration_minutes, 1)                    as p95_duration_minutes,
    round(a.min_duration_minutes, 1)                    as min_duration_minutes,
    round(a.max_duration_minutes, 1)                    as max_duration_minutes,

    -- The spread between the typical and the bad case. A route with a median of 90
    -- minutes and a p95 of 300 is unreliable in a way an average would hide entirely.
    round(a.p95_duration_minutes - a.median_duration_minutes, 1)
                                                        as duration_spread_minutes,

    round(a.avg_speed_kmh, 2)                           as avg_speed_kmh,
    round(a.avg_fuel_consumed_l, 2)                     as avg_fuel_consumed_l,

    case
        when a.avg_distance_km > 0
            then round(a.avg_fuel_consumed_l / a.avg_distance_km * 100, 2)
        else null
    end                                                 as avg_l_per_100km,

    a.overspeed_event_count,
    a.overheating_event_count

from aggregated a
join routes r on a.route_id = r.route_id
