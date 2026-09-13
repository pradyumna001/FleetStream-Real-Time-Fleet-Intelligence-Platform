# Telemetry simulator / Kafka producer.
#
# A small, plain Python image - no Spark, no JVM. The simulator is a producer and
# nothing more, and keeping it separate means it can be scaled, restarted or pointed
# at a different broker without touching the processing images.
FROM python:3.12-slim

# 3.12 rather than 3.13: confluent-kafka ships prebuilt wheels for it, so the image
# builds without a C toolchain and without librdkafka system packages.

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/fleetstream/src

WORKDIR /opt/fleetstream

COPY pyproject.toml ./
COPY src ./src

RUN set -eux; \
    pip install --no-cache-dir --upgrade pip; \
    pip install --no-cache-dir "confluent-kafka>=2.15.0" "PyYAML>=6.0.2"; \
    pip install --no-cache-dir --no-deps .

# The run manifest lands here for the verification suite to read. A named volume is
# mounted over it so verify (running in a different container) can see the file.
RUN mkdir -p /manifests

# Runs as a non-root user: the simulator needs no privileges, and the image is the
# obvious place to enforce that rather than relying on compose to remember.
RUN useradd --create-home --uid 1000 fleetstream \
    && chown -R fleetstream:fleetstream /opt/fleetstream /manifests
USER fleetstream

ENTRYPOINT ["python", "-m", "fleetstream.simulator.producer"]
CMD ["--minutes", "10"]
