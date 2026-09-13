# FleetStream — single entry point.
#
# Compose is split into a base file plus profile-gated overlays so a laptop can run
# a subset of the platform. These variables assemble the right file list; always go
# through make rather than calling docker compose directly.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE_DIR  := compose
BASE         := -f $(COMPOSE_DIR)/docker-compose.yml
SPARK        := $(BASE) -f $(COMPOSE_DIR)/compose.spark.yml
SERVING      := $(BASE) -f $(COMPOSE_DIR)/compose.serving.yml
ORCHESTRATE  := $(BASE) -f $(COMPOSE_DIR)/compose.orchestration.yml
ALL          := $(BASE) -f $(COMPOSE_DIR)/compose.spark.yml -f $(COMPOSE_DIR)/compose.serving.yml -f $(COMPOSE_DIR)/compose.orchestration.yml

DC := docker compose

.PHONY: help
help: ## Show available targets
	@echo "FleetStream — real-time fleet intelligence platform"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "First run:  make preflight && make demo"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

.PHONY: preflight
preflight: ## Check Docker, .env and available memory before anything else
	@echo "==> Docker daemon"
	@docker info >/dev/null 2>&1 || { \
	  echo "    ERROR: cannot reach the Docker daemon."; \
	  echo "    Start Docker Desktop, wait for 'Engine running', then retry."; \
	  exit 1; }
	@echo "    ok - server $$(docker version --format '{{.Server.Version}}')"
	@$(MAKE) --no-print-directory env
	@echo "==> Docker disk usage"
	@docker system df 2>/dev/null | tail -n +1 | sed 's/^/    /' || true
	@echo "==> Memory available to Docker"
	@docker run --rm alpine:3 free -m 2>/dev/null | awk '/Mem:/ {printf "    %d MB total, %d MB available\n", $$2, $$7}' || echo "    (could not determine)"
	@echo "Preflight passed."

.PHONY: env
env: ## Create .env from .env.example if missing
	@if [ ! -f .env ]; then cp .env.example .env; echo "    created .env from .env.example"; else echo "    ok - .env present"; fi

# ---------------------------------------------------------------------------
# Core stack
# ---------------------------------------------------------------------------

.PHONY: up
up: env ## Start core infrastructure (Kafka, MinIO, catalog DB, Iceberg REST)
	$(DC) $(BASE) up -d kafka kafka-init minio minio-init catalog-db iceberg-rest
	@echo ""
	@echo "  Kafka          localhost:9092"
	@echo "  MinIO console  http://localhost:9001"
	@echo "  Iceberg REST   http://localhost:8181"

.PHONY: build
build: env ## Build the Spark, simulator and Airflow images
	$(DC) $(ALL) build

.PHONY: bootstrap
bootstrap: env ## Create Iceberg namespaces/tables and run the round-trip smoke test
	$(DC) $(SPARK) run --rm spark-bootstrap

.PHONY: seed
seed: env ## Load the vehicle, driver and route dimensions
	$(DC) $(SPARK) run --rm spark-seed

.PHONY: maintenance
maintenance: env ## Compact Iceberg tables and expire old snapshots
	$(DC) $(SPARK) --profile maintenance run --rm spark-maintenance

.PHONY: stream
stream: env ## Start the Spark cluster and all streaming queries
	$(DC) $(SPARK) --profile stream up -d
	@echo "  Spark master UI   http://localhost:8080"
	@echo "  bronze driver UI  http://localhost:4040"
	@echo "  silver driver UI  http://localhost:4041"

.PHONY: serve
serve: env ## Start Trino, Prometheus and Grafana
	$(DC) $(SERVING) --profile serve up -d
	@echo "  Trino    http://localhost:8090"
	@echo "  Grafana  http://localhost:3000"

.PHONY: orchestrate
orchestrate: env ## Start Airflow
	$(DC) $(ORCHESTRATE) --profile orchestrate up -d
	@echo "  Airflow  http://localhost:8082"

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

.PHONY: simulate
simulate: env ## Produce telemetry to Kafka continuously in the background
	$(DC) $(BASE) --profile simulate up -d simulator
	@echo "  simulator running - follow with: make logs-simulator"

.PHONY: simulate-once
simulate-once: env ## Produce a bounded batch of telemetry and exit
	$(DC) $(BASE) run --rm simulator

.PHONY: consume
consume: ## Print the first few telemetry messages
	$(DC) $(BASE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic fleet.telemetry --from-beginning --max-messages 5

.PHONY: lag
lag: ## Show consumer-group lag per partition
	$(DC) $(BASE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --all-groups

.PHONY: topics
topics: ## Describe Kafka topics and their partitions
	$(DC) $(BASE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe

# ---------------------------------------------------------------------------
# Transformation and verification
# ---------------------------------------------------------------------------

.PHONY: dbt
dbt: ## Build the Gold models with dbt on Trino
	$(DC) $(ORCHESTRATE) run --rm dbt build

.PHONY: dbt-test
dbt-test: ## Run dbt tests only (grain, uniqueness, referential integrity)
	$(DC) $(ORCHESTRATE) run --rm dbt test

.PHONY: verify
verify: ## Run the end-to-end assertion suite
	$(DC) $(SPARK) --profile verify run --rm spark-verify

.PHONY: test
test: ## Run host unit tests (no Spark required)
	PYTHONPATH=src python -m pytest tests/unit -v

.PHONY: test-integration
test-integration: ## Run integration tests inside the Spark container
	$(DC) $(SPARK) run --rm --entrypoint python3 spark-verify -m pytest /opt/fleetstream/tests/integration -v

.PHONY: lint
lint: ## Ruff + mypy
	python -m ruff check src tests
	python -m mypy src

.PHONY: format
format: ## Auto-format with ruff
	python -m ruff format src tests
	python -m ruff check --fix src tests

# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------

.PHONY: ps
ps: ## Show running services
	$(DC) $(ALL) ps

.PHONY: logs
logs: ## Tail logs for all running services
	$(DC) $(ALL) logs -f --tail=100

.PHONY: logs-simulator
logs-simulator: ## Tail simulator logs
	$(DC) $(BASE) logs -f --tail=100 simulator

.PHONY: logs-bronze
logs-bronze: ## Tail the Bronze ingestion query
	$(DC) $(SPARK) logs -f --tail=100 bronze-ingest

.PHONY: logs-silver
logs-silver: ## Tail the Silver transformation query
	$(DC) $(SPARK) logs -f --tail=100 silver-transform

.PHONY: sql
sql: ## Open a Trino shell against the lakehouse
	$(DC) $(SERVING) exec trino trino --catalog iceberg --schema silver

.PHONY: down
down: ## Stop all services, keeping data volumes
	$(DC) $(ALL) --profile stream --profile serve --profile orchestrate --profile simulate down --remove-orphans

.PHONY: clean
clean: ## Stop everything and DELETE all data volumes
	@echo "This deletes every volume: Kafka logs, MinIO objects, the Iceberg catalog and all checkpoints."
	@read -p "Type 'yes' to continue: " ans && [ "$$ans" = "yes" ] || { echo "aborted"; exit 1; }
	$(DC) $(ALL) --profile stream --profile serve --profile orchestrate --profile simulate down -v --remove-orphans

.PHONY: demo
demo: ## Full path: core -> bootstrap -> seed -> streaming -> simulate -> serving
	$(MAKE) up
	$(MAKE) bootstrap
	$(MAKE) seed
	$(MAKE) stream
	$(MAKE) simulate
	$(MAKE) serve
	@echo ""
	@echo "Stack is running. Give it a minute, then: make verify"
