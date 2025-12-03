#!/bin/bash
echo "🧹 Xóa dữ liệu forecast cũ..."

# Use Trino to drop tables (if they exist)
docker-compose exec trino trino --server localhost:8080 --user admin --execute "
-- Drop forecast tables
DROP TABLE IF EXISTS platinum.demand_forecast_revenue;
DROP TABLE IF EXISTS platinum.demand_forecast_quantity;
DROP TABLE IF EXISTS platinum.demand_forecast;
DROP TABLE IF EXISTS platinum.forecast_monitoring;
DROP TABLE IF EXISTS platinum.forecast_backtest_detail;

-- Optionally drop features table to rebuild from scratch
-- DROP TABLE IF EXISTS silver.forecast_features;
-- DROP TABLE IF EXISTS silver.forecast_series_stats;
"

echo "✅ Đã xóa các bảng forecast cũ"
