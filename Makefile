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