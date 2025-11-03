# Load environment variables
include .env
export $(shell sed 's/=.*//' .env)

# Build all services
build:
	docker compose --env-file .env build

# Start all services (detached)
up:
	docker compose --env-file .env up -d

# Stop and remove all services
down:
	docker compose --env-file .env down

# Remove containers, networks, volumes (use with caution)
down-clean:
	docker compose --env-file .env down -v

# Restart (stop -> up)
restart: down up

# Show logs (follow)
logs:
	docker compose --env-file .env logs -f

# Rebuild and restart everything
rebuild:
	docker compose --env-file .env build --no-cache
	docker compose --env-file .env up -d --force-recreate

# Connect to Dagster MySQL database
to_dagster_mysql:
	docker exec -it de_mysql \
		mysql --local-infile=1 -u"${DAGSTER_MYSQL_USERNAME}" -p"${DAGSTER_MYSQL_PASSWORD}" ${DAGSTER_MYSQL_DB}

# Connect to MySQL (user)
to_mysql:
	docker exec -it de_mysql \
		mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

# Connect to MySQL (root)
to_mysql_root:
	docker exec -it de_mysql \
		mysql --local-infile=1 -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

# Shell into containers
to_etl:
	docker exec -it etl_pipeline bash

to_dagster:
	docker exec -it de_dagster bash

# Run ETL jobs manually (nếu bạn đã define job/commands trong container)
etl_bronze:
	docker exec -it etl_pipeline dagster job execute -m etl_pipeline.job.reload_data

# Full pipeline
etl_all: etl_bronze
	@echo "✅ ETL pipeline executed (requested jobs)."

run_mysql_to_minio:
	docker exec -it spark-master \
		spark-submit \
		--packages com.mysql:mysql-connector-j:8.0.33 \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minio \
		--conf spark.hadoop.fs.s3a.secret.key=minio123 \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		/opt/dagster/app/etl_pipeline/job/mysql_to_minio.py

run_mysql_to_minio_all:
	docker exec -it spark-master \
		spark-submit \
		--packages com.mysql:mysql-connector-j:8.0.33 \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minio \
		--conf spark.hadoop.fs.s3a.secret.key=minio123 \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		/opt/dagster/app/etl_pipeline/job/mysql_to_minio.py

create_bucket:
	docker exec -it minio \
		mc alias set local http://127.0.0.1:9000 minio minio123 && \
		mc mb local/lakehouse || true

# ============================================================
# FORECASTING COMMANDS
# ============================================================

.PHONY: forecast-init
forecast-init:
	@echo "🔧 Initializing MLflow database..."
	@bash scripts/init_mlflow.sh
	@echo ""
	@echo "🔧 Creating forecast_monitoring table..."
	@bash scripts/init_forecast_monitoring.sh

.PHONY: forecast-features
forecast-features:
	@echo "🔨 Building forecast features..."
	docker exec etl_pipeline python -m etl_pipeline.ml.feature_build
	@echo "✅ Features built"

.PHONY: forecast-train
forecast-train:
	@echo "🤖 Training forecasting model..."
	docker exec etl_pipeline python -m etl_pipeline.ml.train_models
	@echo "✅ Model trained (check MLflow for run_id)"

.PHONY: forecast-predict
forecast-predict:
	@echo "🔮 Generating forecasts..."
	@if [ -z "$(RUN_ID)" ]; then \
		echo "❌ Error: RUN_ID not provided"; \
		echo "Usage: make forecast-predict RUN_ID=<mlflow_run_id>"; \
		exit 1; \
	fi
	docker exec etl_pipeline python -m etl_pipeline.ml.batch_predict $(RUN_ID)
	@echo "✅ Forecasts generated"

.PHONY: forecast-full
forecast-full:
	@echo "🚀 Running full forecast pipeline via Dagster..."
	docker exec etl_pipeline dagster job launch -j forecast_job
	@echo "✅ Forecast job launched (check Dagster UI at http://localhost:3001)"

.PHONY: forecast-check
forecast-check:
	@echo "📊 Checking forecast tables..."
	docker exec trino trino --execute "SHOW TABLES FROM lakehouse.silver LIKE 'forecast%';"
	docker exec trino trino --execute "SHOW TABLES FROM lakehouse.platinum LIKE 'demand%';"
	@echo "✅ Check complete"

.PHONY: forecast-rebuild
forecast-rebuild:
	@echo "🔄 Rebuilding etl_pipeline container..."
	docker compose build etl_pipeline
	docker compose up -d etl_pipeline
	@echo "✅ Container rebuilt"