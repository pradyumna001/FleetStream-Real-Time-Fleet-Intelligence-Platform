{{ config(materialized='table', tags=['mart', 'dimension']) }}

-- A conformed date spine so daily reports show days with no activity as zero
-- rather than omitting them. A missing row and a genuine zero look identical in a
-- chart, and only one of them is good news.

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast(current_date + interval '1' day as date)"
    ) }}

)

select
    cast(date_day as date)                          as date_key,
    date_day,
    year(date_day)                                  as year,
    quarter(date_day)                               as quarter,
    month(date_day)                                 as month,
    day(date_day)                                   as day_of_month,
    day_of_week(date_day)                           as day_of_week,
    week_of_year(date_day)                          as week_of_year,
    format_datetime(date_day, 'MMMM')               as month_name,
    format_datetime(date_day, 'EEEE')               as day_name,
    day_of_week(date_day) in (6, 7)                 as is_weekend

from spine
