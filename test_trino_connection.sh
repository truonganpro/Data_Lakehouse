#!/bin/bash

# Script kiểm tra kết nối Trino và dữ liệu trong các layer

echo "🔍 Kiểm tra kết nối Trino và dữ liệu..."
echo "========================================="

# Kiểm tra Trino container có chạy không
echo "1. Kiểm tra Trino container..."
if docker ps | grep -q "trino"; then
    echo "✅ Trino container đang chạy"
else
    echo "❌ Trino container không chạy. Hãy chạy: docker-compose up -d trino"
    exit 1
fi

# Kiểm tra Hive Metastore
echo "2. Kiểm tra Hive Metastore..."
if docker ps | grep -q "hive-metastore"; then
    echo "✅ Hive Metastore đang chạy"
else
    echo "❌ Hive Metastore không chạy. Hãy chạy: docker-compose up -d hive-metastore"
    exit 1
fi

# Kiểm tra MinIO
echo "3. Kiểm tra MinIO..."
if docker ps | grep -q "minio"; then
    echo "✅ MinIO đang chạy"
else
    echo "❌ MinIO không chạy. Hãy chạy: docker-compose up -d minio"
    exit 1
fi

# Kiểm tra kết nối Trino
echo "4. Kiểm tra kết nối Trino..."
sleep 5  # Đợi service khởi động

docker exec trino trino --execute "SHOW CATALOGS;" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ Trino kết nối thành công"
else
    echo "❌ Không thể kết nối tới Trino"
    exit 1
fi

# Kiểm tra dữ liệu trong các layer
echo "5. Kiểm tra dữ liệu trong các layer..."
echo "   - Bronze layer:"
docker exec trino trino --execute "USE lakehouse.bronze; SHOW TABLES;" 2>/dev/null || echo "   ⚠️  Bronze layer chưa có dữ liệu"

echo "   - Silver layer:"
docker exec trino trino --execute "USE lakehouse.silver; SHOW TABLES;" 2>/dev/null || echo "   ⚠️  Silver layer chưa có dữ liệu"

echo "   - Gold layer:"
docker exec trino trino --execute "USE lakehouse.gold; SHOW TABLES;" 2>/dev/null || echo "   ⚠️  Gold layer chưa có dữ liệu"

echo "   - Platinum layer:"
docker exec trino trino --execute "USE lakehouse.platinum; SHOW TABLES;" 2>/dev/null || echo "   ⚠️  Platinum layer chưa có dữ liệu"

echo ""
echo "6. Kiểm tra sample data trong Platinum layer..."
docker exec trino trino --execute "USE lakehouse.platinum; SELECT COUNT(*) as record_count FROM dmsalesmonthlycategory;" 2>/dev/null || echo "   ⚠️  Chưa có dữ liệu trong datamart"

echo ""
echo "🎯 Hướng dẫn tiếp theo:"
echo "   1. Nếu các layer chưa có dữ liệu, chạy ETL pipeline trong Dagster: http://localhost:3001"
echo "   2. Sau đó cấu hình Metabase theo hướng dẫn trong METABASE_TRINO_SETUP.md"
echo "   3. Truy cập Metabase: http://localhost:3000"
