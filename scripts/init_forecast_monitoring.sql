-- Initialize forecast_monitoring table for Forecasting System
-- Run via: docker exec spark-master spark-sql -f /path/to/this/file
-- Or via Trino/Spark SQL

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

