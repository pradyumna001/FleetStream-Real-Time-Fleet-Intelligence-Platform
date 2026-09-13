{{ config(materialized='table', tags=['mart', 'dimension']) }}

select
    driver_id,
    driver_name,
    driver_category,
    license_type,
    aggression_index,

    case
        when aggression_index < 0.8 then 'cautious'
        when aggression_index < 1.3 then 'normal'
        else 'aggressive'
    end as risk_band

from {{ ref('stg_drivers') }}
