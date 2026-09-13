{{ config(materialized='view', tags=['intermediate', 'incident']) }}

-- Individual readings promoted to incidents.
--
-- An "incident" is one reading that breached a threshold. Consecutive breaches
-- during a single overheating episode therefore produce several incident rows,
-- which is intentional: the grain here is the breach, and episode-level rollups
-- belong in a model that says so.

with telemetry as (

    select * from {{ ref('stg_telemetry') }}
    where {{ incremental_window('event_time') }}

),

classified as (

    select
        event_id,
        vehicle_id,
        driver_id,
        trip_id,
        route_id,
        event_time,
        event_date,
        latitude,
        longitude,
        speed_kmh,
        engine_temperature_c,
        fuel_level_pct,
        battery_level_pct,
        vehicle_type,
        region,
        driver_name,
        driver_category,

        case
            when engine_temperature_c > {{ var('critical_overheat_threshold_c') }}
                then 'engine_critical_overheat'
            when engine_temperature_c > {{ var('overheat_threshold_c') }}
                then 'engine_overheat'
            when speed_kmh > {{ var('severe_overspeed_kmh') }}
                then 'severe_overspeed'
            when is_overspeed
                then 'overspeed'
            when fuel_level_pct < {{ var('low_fuel_threshold_pct') }}
                then 'low_fuel'
            else null
        end as incident_type

    from telemetry

)

select
    -- Deterministic key: the same reading always yields the same incident id, so
    -- reprocessing a day merges rather than duplicates.
    {{ dbt_utils.generate_surrogate_key(['event_id', 'incident_type']) }} as incident_id,
    *,
    case
        when incident_type = 'engine_critical_overheat' then 'critical'
        when incident_type in ('engine_overheat', 'severe_overspeed') then 'high'
        when incident_type in ('overspeed', 'low_fuel') then 'medium'
        else 'low'
    end as severity

from classified
where incident_type is not null
