#!/bin/bash
# Script kiểm tra và đăng ký demand_forecast table

echo "🔍 Kiểm tra demand_forecast table..."
echo ""

# 1. Kiểm tra dữ liệu trong MinIO
echo "1️⃣ Kiểm tra dữ liệu trong MinIO:"
if [ -d "minio/lakehouse/platinum/demand_forecast" ]; then
    echo "   ✅ Dữ liệu tồn tại: minio/lakehouse/platinum/demand_forecast"
    echo "   📊 Số file: $(find minio/lakehouse/platinum/demand_forecast -type f | wc -l)"
else
    echo "   ❌ Không tìm thấy dữ liệu"
    exit 1
fi
echo ""

# 2. Kiểm tra trong Hive Metastore (Spark)
echo "2️⃣ Kiểm tra trong Hive Metastore (Spark SQL):"
docker-compose exec -T spark-master spark-sql << 'SPARK_SQL' 2>/dev/null | grep -E "demand_forecast|Time taken"
SET spark.hadoop.fs.s3a.endpoint=http://minio:9000;
SET spark.hadoop.fs.s3a.access.key=minio;
SET spark.hadoop.fs.s3a.secret.key=minio123;
SET spark.hadoop.fs.s3a.path.style.access=true;
SHOW TABLES IN platinum;
SPARK_SQL

if [ $? -eq 0 ]; then
    echo "   ✅ Table có trong Hive Metastore"
else
    echo "   ❌ Table không có trong Hive Metastore"
fi
echo ""

# 3. Kiểm tra trong Trino (Hive catalog)
echo "3️⃣ Kiểm tra trong Trino (Hive catalog):"
TABLES=$(docker-compose exec -T trino trino --server localhost:8080 --catalog hive --schema platinum --execute "SHOW TABLES;" 2>/dev/null | grep -i forecast)
if [ -n "$TABLES" ]; then
    echo "   ✅ Table có trong Trino Hive catalog: $TABLES"
else
    echo "   ⚠️  Table chưa có trong Trino Hive catalog"
fi
echo ""

# 4. Kiểm tra trong Trino (Delta Lake catalog)
echo "4️⃣ Kiểm tra trong Trino (Delta Lake catalog):"
TABLES=$(docker-compose exec -T trino trino --server localhost:8080 --catalog lakehouse --schema platinum --execute "SHOW TABLES;" 2>/dev/null | grep -i forecast)
if [ -n "$TABLES" ]; then
    echo "   ✅ Table có trong Trino Delta Lake catalog: $TABLES"
else
    echo "   ⚠️  Table chưa có trong Trino Delta Lake catalog"
fi
echo ""

# 5. Hướng dẫn sử dụng trong Metabase
echo "📋 HƯỚNG DẪN SỬ DỤNG TRONG METABASE:"
echo ""
echo "1. Truy cập Metabase: http://localhost:3000"
echo ""
echo "2. Nếu table có trong Hive catalog:"
echo "   → Query: SELECT * FROM hive.platinum.demand_forecast LIMIT 10;"
echo ""
echo "3. Nếu table có trong Delta Lake catalog:"
echo "   → Query: SELECT * FROM lakehouse.platinum.demand_forecast LIMIT 10;"
echo ""
echo "4. Để refresh schema trong Metabase:"
echo "   → Vào Admin Settings → Databases"
echo "   → Chọn database Trino"
echo "   → Click 'Sync database schema now'"
echo ""
echo "5. Nếu vẫn không thấy, thử tạo database connection mới:"
echo "   → Type: Trino"
echo "   → Host: trino"
echo "   → Port: 8080"
echo "   → Catalog: hive (hoặc lakehouse)"
echo "   → Schema: platinum"
echo ""

