{#
    The trailing event-time window an incremental model reprocesses on each run.

    Returns a SQL predicate, or `true` on a full refresh / first build.

    Why a window rather than "everything since the last run": an event that was
    generated yesterday but delivered today carries yesterday's event_time. A filter
    of `event_time > last_max_event_time` would never see it, and yesterday's numbers
    would stay permanently short. Reprocessing a trailing window and merging on a
    deterministic key fixes that, at the cost of recomputing a couple of days.
#}
{% macro incremental_window(column='event_time') %}
    {%- if is_incremental() -%}
        {{ column }} >= current_date - interval '{{ var("lookback_days") }}' day
    {%- else -%}
        true
    {%- endif -%}
{% endmacro %}
