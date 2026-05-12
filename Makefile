.PHONY: up down produce stream dbt-run dbt-test dbt-docs logs clean help

DOCKER_COMPOSE = docker-compose
PYTHON = python3

## ─── INFRASTRUCTURE ─────────────────────────────────────────────────────────

up: ## Start all services (Kafka, Spark, Postgres, Airflow, Grafana)
	@echo "🚀 Starting FinStream services..."
	$(DOCKER_COMPOSE) up -d --build
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 15
	@echo ""
	@echo "✅ FinStream is running!"
	@echo "   Kafka UI    → http://localhost:8080"
	@echo "   Spark UI    → http://localhost:4040"
	@echo "   Airflow     → http://localhost:8090  (admin / finstream)"
	@echo "   Grafana     → http://localhost:3000  (admin / finstream)"
	@echo ""
	@echo "Next steps:"
	@echo "   make produce  → start transaction stream"
	@echo "   make stream   → start Spark anomaly detector"

down: ## Stop all services
	@echo "🛑 Stopping FinStream..."
	$(DOCKER_COMPOSE) down

restart: down up ## Restart all services

## ─── PIPELINE ────────────────────────────────────────────────────────────────

produce: ## Start the Kafka transaction producer
	@echo "📤 Starting transaction producer..."
	$(DOCKER_COMPOSE) exec producer python transaction_generator.py

produce-local: ## Run producer locally (requires local Kafka on 9092)
	@echo "📤 Starting local producer..."
	cd producer && pip install -r requirements.txt -q && python transaction_generator.py

stream: ## Start the PySpark streaming anomaly detector
	@echo "⚡ Starting Spark streaming job..."
	$(DOCKER_COMPOSE) exec spark spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		/app/anomaly_detector.py

stream-local: ## Run Spark job locally
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		spark_streaming/anomaly_detector.py

## ─── DBT ─────────────────────────────────────────────────────────────────────

dbt-run: ## Run all dbt models (Bronze → Silver → Gold)
	@echo "🔧 Running dbt models..."
	$(DOCKER_COMPOSE) run --rm dbt run --profiles-dir . --project-dir .

dbt-test: ## Run dbt data quality tests
	@echo "🧪 Running dbt tests..."
	$(DOCKER_COMPOSE) run --rm dbt test --profiles-dir . --project-dir .

dbt-docs: ## Generate and serve dbt documentation
	@echo "📚 Building dbt docs..."
	$(DOCKER_COMPOSE) run --rm dbt docs generate --profiles-dir . --project-dir .
	$(DOCKER_COMPOSE) run --rm -p 8888:8080 dbt docs serve --profiles-dir . --project-dir .

dbt-bronze: ## Run only Bronze models
	$(DOCKER_COMPOSE) run --rm dbt run --select bronze --profiles-dir . --project-dir .

dbt-silver: ## Run only Silver models
	$(DOCKER_COMPOSE) run --rm dbt run --select silver --profiles-dir . --project-dir .

dbt-gold: ## Run only Gold models
	$(DOCKER_COMPOSE) run --rm dbt run --select gold --profiles-dir . --project-dir .

## ─── MONITORING ──────────────────────────────────────────────────────────────

logs: ## Tail all service logs
	$(DOCKER_COMPOSE) logs -f

logs-producer: ## Tail producer logs
	$(DOCKER_COMPOSE) logs -f producer

logs-spark: ## Tail Spark logs
	$(DOCKER_COMPOSE) logs -f spark

logs-kafka: ## Tail Kafka logs
	$(DOCKER_COMPOSE) logs -f kafka

kafka-topics: ## List Kafka topics
	$(DOCKER_COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consume: ## Tail raw transactions from Kafka (debug)
	$(DOCKER_COMPOSE) exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic raw-transactions \
		--from-beginning \
		--max-messages 10

delta-query: ## Run a quick DuckDB query on Delta tables
	@echo "📊 Querying Delta Lake gold layer..."
	$(DOCKER_COMPOSE) exec spark python3 -c "\
		import duckdb; \
		con = duckdb.connect(); \
		con.execute(\"INSTALL delta; LOAD delta;\"); \
		result = con.execute(\"SELECT risk_level, COUNT(*) as cnt FROM delta_scan('/delta/gold/anomalies') GROUP BY 1 ORDER BY 2 DESC\").fetchdf(); \
		print(result)"

## ─── TESTING ─────────────────────────────────────────────────────────────────

test: ## Run unit tests
	@echo "🧪 Running unit tests..."
	cd spark_streaming && pip install -r requirements.txt -q && pytest tests/ -v

test-coverage: ## Run tests with coverage report
	cd spark_streaming && pytest tests/ --cov=. --cov-report=term-missing -v

lint: ## Run linting (flake8 + black check)
	black --check producer/ spark_streaming/
	flake8 producer/ spark_streaming/ --max-line-length=100

format: ## Auto-format code with black
	black producer/ spark_streaming/

## ─── UTILITIES ───────────────────────────────────────────────────────────────

clean: ## Remove all containers, volumes, and cached data
	@echo "🧹 Cleaning up..."
	$(DOCKER_COMPOSE) down -v --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

reset-delta: ## Clear Delta Lake storage (WARNING: deletes all data)
	@read -p "⚠️  This deletes all Delta Lake data. Are you sure? [y/N] " confirm && \
		[ "$$confirm" = "y" ] && \
		$(DOCKER_COMPOSE) exec spark rm -rf /delta/bronze /delta/silver /delta/gold /delta/checkpoints && \
		echo "Delta Lake cleared." || echo "Aborted."

help: ## Show this help message
	@echo ""
	@echo "FinStream — Real-Time Anomaly Detection Pipeline"
	@echo "================================================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
