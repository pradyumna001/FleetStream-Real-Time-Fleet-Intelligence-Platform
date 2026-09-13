# ADR 0005: Trino locally, Athena in AWS; Snowflake defined but untested

**Status:** accepted

## Context

The design called for Snowflake and Athena as serving layers. Both are paid managed
services and neither runs locally, so the platform could not be demonstrated
end-to-end without an account and a bill.

## Decision

Trino is the local serving engine and the stand-in for Athena. The dbt project defines
a `snowflake` target that is written but not exercised.

## Why Trino substitutes for Athena honestly

Athena is Presto-derived; Trino is the direct continuation of Presto. They share a SQL
dialect, so models written here port to Athena by changing the catalog binding from
the Iceberg REST catalog to Glue - not by rewriting SQL. The same cannot be said of
substituting, say, DuckDB, where the dialect differences would be papered over by the
dbt adapter and only surface on migration.

## On Snowflake

The target exists in `profiles.yml` to make a specific claim testable: that switching
serving engines is a profile change rather than a modelling change. But it has never
been run - there is no account behind it - so it is documented as a starting point,
not presented as working. Some models would likely need adjustment (Trino's
`approx_percentile` and `count_if` have Snowflake equivalents with different names).

Claiming it works because it parses would be the kind of assertion this project is
built to avoid.

## Grafana for both dashboards

Grafana serves the platform-health dashboards (Prometheus) and the business
dashboards (Trino). One tool rather than two because adding a separate BI stack would
cost gigabytes of containers to demonstrate a capability Grafana already covers over
a SQL datasource. In a real deployment the business layer would more likely be
Superset, Looker or Power BI; nothing about the Gold models depends on that choice.
