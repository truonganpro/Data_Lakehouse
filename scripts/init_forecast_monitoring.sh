#!/bin/bash
# Initialize forecast_monitoring table

echo "🔧 Creating platinum.forecast_monitoring table..."

# Method 1: Via Spark SQL (preferred - ensures Delta properties)
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

echo "✅ Table created successfully"
echo ""
echo "📊 Verify table:"
echo "   docker exec trino trino --execute \"DESCRIBE lakehouse.platinum.forecast_monitoring;\""
