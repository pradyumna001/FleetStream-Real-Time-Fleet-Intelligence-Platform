# ADR 0004: PostgreSQL-backed Iceberg REST catalog

**Status:** accepted (supersedes the SQLite default)

## Context

The local stack originally used `apache/iceberg-rest-fixture` with its default SQLite
backend. Under real load this failed:

```
SQLiteException: [SQLITE_BUSY] The database file is locked
ICEBERG_CATALOG_ERROR: Failed to create namespace 'intermediate'
ICEBERG_COMMIT_ERROR: Failed to commit the transaction during insert
```

A dbt run finished with 3 of 72 nodes failing, non-deterministically.

## Cause

A catalog is shared mutable state that every engine writes to. In this platform that
means three Spark streaming queries committing snapshots on 30- and 60-second
triggers, while Trino and dbt create schemas, tables and views - dbt with four
threads. SQLite takes a database-wide write lock and, by default, fails a blocked
writer immediately rather than waiting.

## Options considered

**WAL plus `busy_timeout`.** Tried first, as the smallest change. It helped
substantially - the failure went from "cannot create any namespace" to 3 failures out
of 72 - but did not eliminate the problem. Concurrent writers still collided. Rejected
as insufficient rather than untried.

**Reduce dbt to one thread.** Would likely have worked, by removing the concurrency
instead of supporting it. Rejected: it makes the demo slower to hide a limitation
rather than fix it, and the streaming queries would still contend with each other.

**PostgreSQL backend.** Chosen.

## Decision

Run the REST catalog against PostgreSQL. The stock image bundles only the SQLite JDBC
driver - verified by inspecting the shaded jar - so `docker/iceberg-rest/Dockerfile`
extends it with the PostgreSQL driver and an explicit classpath. The stock image runs
`java -jar`, which ignores `-cp`, so the command names the main class directly
(`org.apache.iceberg.rest.RESTCatalogServer`, taken from the server's own stack
traces).

## Why this is also more realistic

Production Iceberg deployments use Glue or a JDBC catalog on RDS, not an embedded
database. Only the driver and the connection URI differ between this and a real
deployment, so the local stack now exercises the same code path.

## Costs

One more container and one more stateful volume. The custom image must be rebuilt if
the upstream fixture is upgraded. Both are cheap next to a catalog that
non-deterministically drops commits.
