{{ config(materialized='table', tags=['mart', 'dimension']) }}

-- Vehicle dimension. Small and fully refreshed each run: the source is
-- authoritative and cheap to read, so incremental complexity would buy nothing.

select
    vehicle_id,
    vehicle_type,
    manufacturer,
    model,
    manufacturer || ' ' || model            as vehicle_description,
    region,
    fleet_owner,
    max_speed_kmh,
    fuel_capacity_l,
    consumption_l_per_100km,
    overheat_threshold_c,
    has_faulty_cooling,

    case
        when consumption_l_per_100km < 12 then 'efficient'
        when consumption_l_per_100km < 22 then 'standard'
        else 'heavy'
    end                                     as consumption_band

from {{ ref('stg_vehicles') }}
