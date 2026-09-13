# dbt runner.
#
# A separate image rather than dbt installed into Airflow. dbt-core and Airflow both
# pin Jinja2, click and typing-extensions, and installing them together produces a
# resolvable-but-fragile environment that breaks on the next upgrade of either.
# Keeping them apart means each can be upgraded independently, and Airflow invokes
# this image through the Docker API instead of importing dbt.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/dbt \
    DBT_TARGET=trino

WORKDIR /dbt

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends git; \
    rm -rf /var/lib/apt/lists/*; \
    pip install --no-cache-dir "dbt-core==1.12.4" "dbt-trino==1.10.3"

# Packages are installed at build time so a run never depends on dbt Hub being up.
COPY dbt/fleetstream/packages.yml dbt/fleetstream/dbt_project.yml ./
RUN dbt deps --no-version-check || true

COPY dbt/fleetstream/ ./

ENTRYPOINT ["dbt"]
CMD ["build"]
