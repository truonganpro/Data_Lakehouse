#!/bin/bash
# ===================================================================
# 🚀 RUN COMPLETE FORECASTING PIPELINE
# ===================================================================
# This script runs the full forecasting pipeline from start to finish
# Usage: bash scripts/run_forecast.sh

set -e  # Exit on error

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║       🚀 FORECASTING PIPELINE - AUTOMATED EXECUTION                 ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# ===================================================================
# STEP 1: Rebuild etl_pipeline container
# ===================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 STEP 1/6: Rebuild etl_pipeline container..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose build etl_pipeline
docker compose up -d etl_pipeline
echo "✅ Container rebuilt and started"
echo ""

# Wait for services
echo "⏳ Waiting for services to be ready (30 seconds)..."
sleep 30
echo ""

# ===================================================================
# STEP 2: Initialize MLflow database and monitoring table
# ===================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔧 STEP 2/6: Initialize MLflow & monitoring table..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Init MLflow
echo "📊 Creating MLflow database..."
docker exec de_mysql mysql -uroot -padmin123 << MYSQL_SQL
CREATE DATABASE IF NOT EXISTS mlflowdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'mlflow'@'%' IDENTIFIED BY 'mlflow';
GRANT ALL PRIVILEGES ON mlflowdb.* TO 'mlflow'@'%';
FLUSH PRIVILEGES;
SELECT 'MLflow database created' AS status;
MYSQL_SQL

# Init monitoring table
echo "📈 Creating forecast_monitoring table..."
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

echo "✅ MLflow & monitoring table initialized"
echo ""

# ===================================================================
# STEP 3: Build features
# ===================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔨 STEP 3/6: Building features..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec etl_pipeline python -m etl_pipeline.ml.feature_build
echo "✅ Features built successfully"
echo ""

# ===================================================================
# STEP 4: Check features
# ===================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔍 STEP 4/6: Verifying features..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec trino trino --execute "
SELECT 
    COUNT(*) AS total_rows,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    COUNT(DISTINCT product_id) AS n_products,
    COUNT(DISTINCT region_id) AS n_regions
FROM lakehouse.silver.forecast_features;
"
echo ""

# ===================================================================
# STEP 5: Train model
# ===================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🤖 STEP 5/6: Training model..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "⚠️  This will take 5-10 minutes depending on data size..."
echo ""

# Capture output to get run_id
TRAIN_OUTPUT=$(docker exec etl_pipeline python -m etl_pipeline.ml.train_models 2>&1)
echo "$TRAIN_OUTPUT"

# Extract run_id from output
RUN_ID=$(echo "$TRAIN_OUTPUT" | grep -oE 'run_id=[a-f0-9]+' | cut -d= -f2 | head -1)

if [ -z "$RUN_ID" ]; then
    echo "❌ Could not extract run_id from training output"
    echo "Please run manually:"
    echo "  make forecast-train"
    echo "  Then: make forecast-predict RUN_ID=<your_run_id>"
    exit 1
fi

echo ""
echo "✅ Model trained successfully!"
echo "📝 Run ID: $RUN_ID"
echo ""

# ===================================================================
# STEP 6: Generate forecasts
# ===================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔮 STEP 6/6: Generating forecasts..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Using run_id: $RUN_ID"
echo ""
docker exec etl_pipeline python -m etl_pipeline.ml.batch_predict $RUN_ID
echo "✅ Forecasts generated successfully"
echo ""

# ===================================================================
# FINAL: Check results
# ===================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 FINAL: Checking results..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📈 Forecast table:"
docker exec trino trino --execute "
SELECT 
    COUNT(*) AS total_forecasts,
    MIN(forecast_date) AS first_forecast,
    MAX(forecast_date) AS last_forecast,
    MIN(horizon) AS min_horizon,
    MAX(horizon) AS max_horizon
FROM lakehouse.platinum.demand_forecast;
"
echo ""

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                    ✅ PIPELINE COMPLETED SUCCESSFULLY!               ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Next steps:"
echo "   1. View forecasts in Metabase: http://localhost:3000"
echo "   2. Query via Trino:"
echo "      docker exec trino trino --execute \\"
echo "        'SELECT * FROM lakehouse.platinum.demand_forecast LIMIT 10;'\\"
echo ""
echo "   3. Run monitoring (after having actual data for tomorrow):"
echo "      Via Dagster UI: http://localhost:3001 → forecast_job"
echo ""
echo "   4. Schedule daily runs:"
echo "      Enable 'daily_forecast_schedule' in Dagster UI"
echo ""
echo "🎉 Forecasting system is now operational!"

