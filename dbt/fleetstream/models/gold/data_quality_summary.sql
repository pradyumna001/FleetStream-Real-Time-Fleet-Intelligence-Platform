{{ config(materialized='view', tags=['gold', 'quality']) }}

-- Data quality as a first-class reportable dataset, not just log output.
--
-- Two things make this useful rather than decorative: the rejection rate is shown
-- against the volume that succeeded, so a spike is visible as a proportion rather
-- than an absolute number that grows with traffic; and rejections are broken down by
-- rule, because "quality dropped" is not actionable while "the fuel_level_range rule
-- started firing on 4% of records" is.

with quarantined as (

    select
        quarantine_date,
        rule_name,
        rule_severity,
        count(*)                            as violation_count,
        count(distinct event_id)            as rejected_event_count
    from {{ ref('stg_quarantine') }}
    group by quarantine_date, rule_name, rule_severity

),

accepted as (

    select
        event_date,
        count(*)                            as accepted_event_count
    from {{ ref('stg_telemetry') }}
    group by event_date

)

select
    q.quarantine_date                                   as report_date,
    q.rule_name,
    q.rule_severity,
    q.violation_count,
    q.rejected_event_count,
    coalesce(a.accepted_event_count, 0)                 as accepted_event_count,

    case
        when coalesce(a.accepted_event_count, 0) + q.rejected_event_count > 0
            then round(
                q.rejected_event_count * 100.0
                / (a.accepted_event_count + q.rejected_event_count), 4)
        else null
    end                                                 as rejection_rate_pct

from quarantined q
left join accepted a on q.quarantine_date = a.event_date
