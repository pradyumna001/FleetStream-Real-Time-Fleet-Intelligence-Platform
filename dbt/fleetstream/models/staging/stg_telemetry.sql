{{
    config(
        materialized='view',
        tags=['staging', 'telemetry']
    )
}}

-- Staging does renaming, light typing and derived flags only. No joins, no
-- aggregation, no filtering of real records: keeping this layer a thin, faithful
-- projection of the source means every later model can be read without also
-- remembering what staging quietly dropped.

with source as (

    select * from {{ source('silver', 'telemetry') }}

),

renamed as (

    select
        -- identifiers
        event_id,
        vehicle_id,
        driver_id,
        trip_id,
        route_id,

        -- timing. event_date is derived here once because it is the partition key
        -- for every downstream daily aggregate, and deriving it per model invites
        -- one of them to use ingestion time by mistake.
        event_time,
        cast(event_time as date)                    as event_date,
        ingestion_time,
        processed_at,

        -- the lateness of this record, which is what the lookback window exists for
        date_diff('second', event_time, ingestion_time) as ingestion_lag_seconds,

        -- measures
        latitude,
        longitude,
        speed                                       as speed_kmh,
        fuel_level                                  as fuel_level_pct,
        engine_temperature                          as engine_temperature_c,
        battery_level                               as battery_level_pct,
        odometer_km,

        -- enrichment carried down from the streaming layer
        vehicle_type,
        manufacturer,
        model,
        region,
        fleet_owner,
        driver_name,
        driver_category,

        -- operational flags, computed in Silver against per-vehicle limits
        coalesce(is_overspeed, false)               as is_overspeed,
        coalesce(is_overheating, false)             as is_overheating,
        coalesce(is_low_fuel, false)                as is_low_fuel,

        -- provenance
        kafka_partition,
        kafka_offset,
        schema_version,
        silver_run_id

    from source

)

select * from renamed
