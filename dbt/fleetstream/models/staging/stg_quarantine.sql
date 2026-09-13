{{
    config(
        materialized='view',
        tags=['staging', 'quality']
    )
}}

-- Rejected records, exposed for reporting. One row per (record, broken rule), so
-- "which rule fires most" is a plain group by.

with source as (

    select * from {{ source('silver', 'telemetry_quarantine') }}

)

select
    quarantine_id,
    event_id,
    vehicle_id,
    event_time,
    rule_name,
    rule_severity,
    failure_reason,
    raw_payload,
    quarantined_at,
    cast(quarantined_at as date) as quarantine_date,
    run_id

from source
