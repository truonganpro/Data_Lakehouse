#!/bin/bash
# ============================================================================
# Register Delta Lake Tables in Trino
# ============================================================================
# This script registers Delta Lake tables in both Hive Metastore and Trino
# Usage: bash scripts/register_tables.sh [table_name]
#   If table_name is provided, only that table will be registered
#   Otherwise, all forecast-related tables will be registered

set -e

TRINO_HOST="${TRINO_HOST:-trino}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_USER="${TRINO_USER:-chatbot}"
TRINO_CATALOG="${TRINO_CATALOG:-lakehouse}"

# Function to register a table in Trino
register_trino_table() {
    local schema=$1
    local table=$2
    local s3_path=$3
    
    echo "📝 Registering ${schema}.${table} in Trino..."
    
    docker-compose exec -T trino trino \
        --server "http://localhost:${TRINO_PORT}" \
        --user "${TRINO_USER}" \
        --catalog "${TRINO_CATALOG}" \
        --execute "CALL system.register_table('${schema}', '${table}', '${s3_path}')" \
        2>&1 | grep -v "WARNING: Unable to create a system terminal" || true
    
    echo "✅ Registered ${schema}.${table} in Trino"
}

# Function to register a table in Hive Metastore (Spark)
register_hive_table() {
    local schema=$1
    local table=$2
    local s3_path=$3
    
    echo "📝 Registering ${schema}.${table} in Hive Metastore..."
    
    docker exec spark-master spark-sql << SPARK_SQL
SET spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension;
SET spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog;
SET spark.hadoop.fs.s3a.endpoint=http://minio:9000;
SET spark.hadoop.fs.s3a.access.key=minio;
SET spark.hadoop.fs.s3a.secret.key=minio123;
SET spark.hadoop.fs.s3a.path.style.access=true;
SET spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem;
SET spark.hadoop.fs.s3a.connection.ssl.enabled=false;
SET spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;

CREATE DATABASE IF NOT EXISTS ${schema} LOCATION 's3a://lakehouse/${schema}';
DROP TABLE IF EXISTS ${schema}.${table};
CREATE TABLE ${schema}.${table}
USING DELTA
LOCATION '${s3_path}';
SHOW TABLES IN ${schema};
SPARK_SQL
    
    echo "✅ Registered ${schema}.${table} in Hive Metastore"
}

# Main registration function
register_table() {
    local schema=$1
    local table=$2
    local s3_path=$3
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔧 Registering: ${schema}.${table}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Register in Hive Metastore first
    register_hive_table "$schema" "$table" "$s3_path"
    
    # Then register in Trino
    register_trino_table "$schema" "$table" "$s3_path"
    
    echo "✅ ${schema}.${table} registration complete"
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║       🚀 REGISTERING DELTA LAKE TABLES                              ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# If specific table is requested
if [ -n "$1" ]; then
    case "$1" in
        demand_forecast)
            register_table "platinum" "demand_forecast" "s3a://lakehouse/platinum/demand_forecast"
            ;;
        demand_forecast_revenue)
            register_table "platinum" "demand_forecast_revenue" "s3a://lakehouse/platinum/demand_forecast_revenue"
            ;;
        demand_forecast_quantity)
            register_table "platinum" "demand_forecast_quantity" "s3a://lakehouse/platinum/demand_forecast_quantity"
            ;;
        forecast_features)
            register_table "silver" "forecast_features" "s3a://lakehouse/silver/forecast_features"
            ;;
        forecast_monitoring)
            register_table "platinum" "forecast_monitoring" "s3a://lakehouse/platinum/forecast_monitoring"
            ;;
        *)
            echo "❌ Unknown table: $1"
            echo "Available tables: demand_forecast, demand_forecast_revenue, demand_forecast_quantity, forecast_features, forecast_monitoring"
            exit 1
            ;;
    esac
else
    # Register all forecast-related tables
    echo "Registering all forecast-related tables..."
    echo ""
    
    register_table "platinum" "demand_forecast" "s3a://lakehouse/platinum/demand_forecast"
    register_table "platinum" "demand_forecast_revenue" "s3a://lakehouse/platinum/demand_forecast_revenue"
    register_table "platinum" "demand_forecast_quantity" "s3a://lakehouse/platinum/demand_forecast_quantity"
    register_table "silver" "forecast_features" "s3a://lakehouse/silver/forecast_features"
    register_table "silver" "forecast_series_stats" "s3a://lakehouse/silver/forecast_series_stats"
    register_table "platinum" "forecast_monitoring" "s3a://lakehouse/platinum/forecast_monitoring"
    register_table "platinum" "forecast_backtest_detail" "s3a://lakehouse/platinum/forecast_backtest_detail"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ ALL TABLES REGISTERED SUCCESSFULLY!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Verify tables:"
echo "   docker exec trino trino --execute \"SHOW TABLES FROM ${TRINO_CATALOG}.platinum;\""
echo "   docker exec trino trino --execute \"SHOW TABLES FROM ${TRINO_CATALOG}.silver;\""
echo ""
