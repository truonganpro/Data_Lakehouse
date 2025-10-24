#!/bin/bash
set -e

echo "🚀 Starting ETL Pipeline..."
echo ""

# Wait for Dagster to be ready
echo "⏳ Waiting for Dagster to be ready..."
for i in {1..30}; do
    if docker exec etl_pipeline dagster instance info > /dev/null 2>&1; then
        echo "✅ Dagster is ready!"
        break
    fi
    echo "  Waiting... ($i/30)"
    sleep 2
done

echo ""
echo "📊 Running Bronze Layer ETL (MySQL → MinIO)..."
docker exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data

echo ""
echo "✅ ETL Complete! Verifying data in MinIO..."
echo ""
echo "📍 Access Points:"
echo "  • Dagster UI:     http://localhost:3001"
echo "  • MinIO Console:  http://localhost:9001 (minio/minio123)"
echo "  • Trino:          http://localhost:8082"
echo "  • Metabase:       http://localhost:3000"
echo "  • Streamlit:      http://localhost:8501"
echo ""
echo "🔍 To verify data in Trino, run:"
echo "  docker exec -it trino trino"
echo "  USE lakehouse.bronze;"
echo "  SHOW TABLES;"
echo "  SELECT COUNT(*) FROM customer;"

