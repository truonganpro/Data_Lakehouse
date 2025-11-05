#!/bin/bash
# Register demand_forecast table in Hive Metastore
# This script registers the existing Delta table in MinIO to Hive Metastore
# so it can be accessed by Trino and Metabase

echo "🔧 Registering demand_forecast table in Hive Metastore..."

# Wait for Hive Metastore to be ready
echo "⏳ Waiting for Hive Metastore..."
until docker exec hive-metastore nc -z localhost 9083 2>/dev/null; do
  echo "   Waiting for Hive Metastore..."
  sleep 2
done
echo "✅ Hive Metastore is ready"

# Register the table using Spark SQL
docker exec spark-master spark-sql << 'SPARK_SQL'
-- Configure Spark for Delta Lake and MinIO
SET spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension;
SET spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog;
SET spark.hadoop.fs.s3a.endpoint=http://minio:9000;
SET spark.hadoop.fs.s3a.access.key=minio;
SET spark.hadoop.fs.s3a.secret.key=minio123;
SET spark.hadoop.fs.s3a.path.style.access=true;
SET spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem;
SET spark.hadoop.fs.s3a.connection.ssl.enabled=false;
SET spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS platinum LOCATION 's3a://lakehouse/platinum';

-- Drop existing table if exists (to avoid conflicts)
DROP TABLE IF EXISTS platinum.demand_forecast;

-- Register the Delta table from MinIO
-- The table location should point to the Delta table in MinIO
CREATE TABLE platinum.demand_forecast
USING DELTA
LOCATION 's3a://lakehouse/platinum/demand_forecast';

-- Verify the table
SHOW TABLES IN platinum;
DESCRIBE EXTENDED platinum.demand_forecast;
SPARK_SQL

echo ""
echo "✅ Table registration completed!"
echo ""
echo "📊 Verify table in Trino:"
echo "   docker exec trino trino --execute \"SHOW TABLES FROM lakehouse.platinum;\""
echo ""
echo "📊 Query sample data:"
echo "   docker exec trino trino --execute \"SELECT * FROM lakehouse.platinum.demand_forecast LIMIT 10;\""

