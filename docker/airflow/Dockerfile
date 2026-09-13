# Airflow image.
#
# Deliberately thin: Airflow orchestrates, it does not process. It holds no Spark, no
# dbt and no pandas. Every heavy step runs in its own container, launched through the
# Docker API, which is why the only additions here are the Docker provider and a
# Trino client for the lightweight SQL checks.
FROM apache/airflow:3.3.1-python3.12

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Constraints keep provider versions compatible with this exact Airflow release.
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir \
      "apache-airflow-providers-docker" \
      "trino[sqlalchemy]" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
