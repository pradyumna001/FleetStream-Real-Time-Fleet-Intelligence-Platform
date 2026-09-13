# Spark 3.5 with the Iceberg and Kafka connectors baked in.
#
# Spark 3.5 (not 4.x) is deliberate: it has the most mature Iceberg runtime, and it
# provides dropDuplicatesWithinWatermark, which the Silver deduplication relies on.
#
# Jars are baked into the image rather than resolved with --packages at submit time.
# Runtime Ivy resolution means every job start depends on Maven Central being
# reachable, adds 30-60s to each submit, and can silently pick a different version
# than the one tested. Pinning them here makes the image the unit of reproducibility.
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

# Versions resolved from the spark-sql-kafka POM for this exact Spark build, so the
# Kafka client and connection-pool versions match what Spark was compiled against.
ARG ICEBERG_VERSION=1.10.1
ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
# Must match the Hadoop bundled in this Spark image (3.3.4); the AWS SDK version
# is the one hadoop-aws 3.3.4 was compiled against. Mismatches here surface as
# NoSuchMethodError deep inside the S3A client.
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV SPARK_HOME=/opt/spark
WORKDIR ${SPARK_HOME}

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl procps; \
    rm -rf /var/lib/apt/lists/*

# Two S3 clients are needed, for two different callers:
#   iceberg-aws-bundle : Iceberg's own S3FileIO, which reads and writes table data.
#   hadoop-aws         : Spark's filesystem layer, which writes streaming checkpoints.
# Both are required because the Silver query streams FROM an Iceberg table, and the
# Iceberg streaming source writes its offset metadata into the checkpoint directory
# through S3FileIO - so the checkpoint URI must be one both clients understand.
RUN set -eux; \
    M=https://repo1.maven.org/maven2; \
    for url in \
      "$M/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_BINARY}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_BINARY}-${ICEBERG_VERSION}.jar" \
      "$M/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "$M/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "$M/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "$M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "$M/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "$M/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      "$M/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    ; do \
      echo "fetching $(basename "$url")"; \
      curl -fsSL --retry 3 --retry-delay 2 -o "${SPARK_HOME}/jars/$(basename "$url")" "$url"; \
    done

# Exposes Spark metrics at /metrics/prometheus on the driver UI for Prometheus.
COPY docker/spark/metrics.properties ${SPARK_HOME}/conf/metrics.properties
COPY docker/spark/log4j2.properties  ${SPARK_HOME}/conf/log4j2.properties

# Application code.
#
# Installed non-editable and with --no-deps. Two deliberate choices:
#   --no-deps : the image already ships PySpark; pip must not replace it.
#   not -e    : the Spark image's pip predates PEP 660, so an editable install
#               of a pyproject-only project fails outright with a missing
#               build_editable hook. A regular install needs no such backend.
COPY pyproject.toml /opt/fleetstream/pyproject.toml
COPY src /opt/fleetstream/src
# Integration tests run inside this image - they need a real JVM and catalog.
COPY tests /opt/fleetstream/tests
RUN set -eux; \
    pip install --no-cache-dir --upgrade pip setuptools wheel; \
    pip install --no-cache-dir PyYAML pytest; \
    pip install --no-cache-dir --no-deps /opt/fleetstream

# PySpark ships inside the image as source plus a py4j zip rather than as a pip
# package, and only spark-submit puts it on sys.path. Adding it here lets plain
# `python3 -m pytest` import pyspark, which is what the integration tests need.
ENV PYTHONPATH=/opt/fleetstream/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYTHONUNBUFFERED=1

# Checkpoints land here; compose mounts a named volume so the filesystem is ext4
# inside the WSL2 VM, with the atomic rename semantics checkpointing requires.
RUN mkdir -p /checkpoints && chown -R 185:0 /checkpoints /opt/fleetstream

USER 185
