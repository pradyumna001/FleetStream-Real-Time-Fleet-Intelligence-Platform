# Iceberg REST catalog backed by PostgreSQL.
#
# WHY NOT THE STOCK IMAGE
#   apache/iceberg-rest-fixture bundles only the SQLite JDBC driver (verified by
#   inspecting the shaded jar). SQLite takes a database-wide write lock, and this
#   platform has three streaming queries committing snapshots while Trino and dbt
#   create schemas and tables. Under that load the catalog returns SQLITE_BUSY and
#   commits fail outright - observed here as dbt models failing with
#   "Failed to create namespace" and "Failed to commit the transaction".
#
#   Enabling WAL and a busy timeout helped substantially but did not eliminate it:
#   concurrent writers still collided. A catalog is shared mutable state written by
#   every engine at once, so it needs a database built for concurrent writers.
#
#   Postgres here mirrors what a real deployment uses - Glue, or a JDBC catalog on
#   RDS. Only the driver and the connection URI differ.
FROM apache/iceberg-rest-fixture:1.10.1

USER root

ARG POSTGRES_JDBC_VERSION=42.7.4
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /usr/lib/iceberg-rest/postgresql.jar
RUN chmod 644 /usr/lib/iceberg-rest/postgresql.jar

USER iceberg

# The stock image runs `java -jar`, which ignores -cp entirely, so the driver has to
# be put on an explicit classpath with the main class named. Taken from the server's
# own stack traces: org.apache.iceberg.rest.RESTCatalogServer.
WORKDIR /usr/lib/iceberg-rest
CMD ["java", \
     "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar", \
     "org.apache.iceberg.rest.RESTCatalogServer"]
