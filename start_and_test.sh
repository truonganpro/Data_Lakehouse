#!/bin/bash

echo "🚀 Starting Data Lakehouse System"
echo "================================="

# Tạo .env file nếu chưa có
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
MYSQL_ROOT_PASSWORD=root123
MYSQL_DATABASE=metastore
MYSQL_USER=hive
MYSQL_PASSWORD=hive
POSTGRES_DB=postgres
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
EOF
    echo "✅ .env file created"
fi

# Khởi động các services cần thiết
echo "🐳 Starting Docker services..."
docker-compose up -d de_mysql minio mc hive-metastore

# Đợi services khởi động
echo "⏳ Waiting for services to be ready..."
sleep 30

# Kiểm tra trạng thái services
echo "🔍 Checking service status..."
docker-compose ps

# Test kết nối MinIO
echo "🔍 Testing MinIO connection..."
docker exec minio mc ls minio/

# Test kết nối MySQL
echo "🔍 Testing MySQL connection..."
docker exec de_mysql mysql -u hive -phive -e "SHOW DATABASES;"

# Khởi động Spark cluster
echo "⚡ Starting Spark cluster..."
docker-compose up -d spark-master spark-worker-1

# Đợi Spark khởi động
echo "⏳ Waiting for Spark to be ready..."
sleep 20

# Test Spark kết nối MinIO
echo "🔍 Testing Spark MinIO connection..."
docker exec spark-master python3 /opt/bitnami/spark/test_spark_minio_connection.py

echo "✅ System startup and test completed!"
echo "🌐 Access URLs:"
echo "   - Spark Master UI: http://localhost:8080"
echo "   - MinIO Console: http://localhost:9001 (minio/minio123)"
echo "   - Metabase: http://localhost:3000"
echo "   - Trino: http://localhost:8082"
