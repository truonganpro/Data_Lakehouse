#!/bin/bash
# ============================================================================
# Initialize Forecasting System
# ============================================================================
# This script initializes MLflow database and forecast_monitoring table
# Usage: bash scripts/init_forecast.sh

set -e

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║       🔧 INITIALIZING FORECASTING SYSTEM                          ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# ============================================================================
# STEP 1: Initialize MLflow Database
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 STEP 1/2: Initializing MLflow database..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

docker exec de_mysql mysql -uroot -padmin123 << MYSQL_EOF
CREATE DATABASE IF NOT EXISTS mlflowdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'mlflow'@'%' IDENTIFIED BY 'mlflow';
GRANT ALL PRIVILEGES ON mlflowdb.* TO 'mlflow'@'%';
FLUSH PRIVILEGES;
SELECT 'MLflow database created successfully' AS status;
MYSQL_EOF

echo "✅ MLflow database initialized"
echo ""

# ============================================================================
# STEP 2: Create forecast_monitoring Table
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📈 STEP 2/2: Creating forecast_monitoring table..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

docker exec spark-master spark-sql << SPARK_SQL
CREATE TABLE IF NOT EXISTS lakehouse.platinum.forecast_monitoring (
  date DATE,
  product_id VARCHAR(100),
  region_id VARCHAR(10),
  horizon INT,
  y_actual DOUBLE,
  yhat DOUBLE,
  yhat_lo DOUBLE,
  yhat_hi DOUBLE,
  abs_error DOUBLE,
  pct_error DOUBLE,
  smape DOUBLE,
  model_name VARCHAR(50),
  run_id VARCHAR(100)
)
USING delta
PARTITIONED BY (date)
LOCATION 's3a://lakehouse/platinum/forecast_monitoring';
SPARK_SQL

echo "✅ forecast_monitoring table created"
echo ""

# ============================================================================
# VERIFICATION
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ FORECASTING SYSTEM INITIALIZED SUCCESSFULLY!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Verification commands:"
echo "  • MLflow: docker exec etl_pipeline python -c \"import mlflow; mlflow.set_tracking_uri('mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb'); print('✅ MLflow connection OK')\""
echo "  • Table:   docker exec trino trino --execute \"DESCRIBE lakehouse.platinum.forecast_monitoring;\""
echo ""
