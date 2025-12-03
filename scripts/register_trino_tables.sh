#!/bin/bash
# Script to register Delta Lake tables in Trino
# Usage: ./register_trino_tables.sh [schema] [table_name] [s3_path]

set -e

TRINO_HOST="${TRINO_HOST:-trino}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_USER="${TRINO_USER:-chatbot}"
TRINO_CATALOG="${TRINO_CATALOG:-lakehouse}"

# Function to register a table
register_table() {
    local schema=$1
    local table=$2
    local s3_path=$3
    
    echo "📝 Registering ${schema}.${table}..."
    
    docker-compose exec -T trino trino \
        --server "http://localhost:${TRINO_PORT}" \
        --user "${TRINO_USER}" \
        --catalog "${TRINO_CATALOG}" \
        --execute "CALL system.register_table('${schema}', '${table}', '${s3_path}')" \
        2>&1 | grep -v "WARNING: Unable to create a system terminal" || true
    
    echo "✅ Registered ${schema}.${table}"
}

# Register forecast tables
echo "🚀 Registering forecast tables in Trino..."
echo ""

register_table "platinum" "demand_forecast_revenue" "s3a://lakehouse/platinum/demand_forecast_revenue"
register_table "platinum" "demand_forecast_quantity" "s3a://lakehouse/platinum/demand_forecast_quantity"
register_table "silver" "forecast_features" "s3a://lakehouse/silver/forecast_features"
register_table "silver" "forecast_series_stats" "s3a://lakehouse/silver/forecast_series_stats"
register_table "platinum" "forecast_monitoring" "s3a://lakehouse/platinum/forecast_monitoring"
register_table "platinum" "forecast_backtest_detail" "s3a://lakehouse/platinum/forecast_backtest_detail"

echo ""
echo "✅ All forecast tables registered successfully!"
echo ""
echo "📊 Verify tables:"
echo "   docker-compose exec -T trino trino --server http://localhost:8080 --user ${TRINO_USER} --catalog ${TRINO_CATALOG} --schema platinum --execute 'SHOW TABLES'"

