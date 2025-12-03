#!/bin/bash
# Script áp dụng các cấu hình tối ưu và restart services

set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     🔧 ÁP DỤNG CẤU HÌNH TỐI ƯU                                ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# 1. Dừng các services cần restart
echo "1️⃣ Dừng các services..."
docker-compose stop de_dagster_dagit de_dagster_daemon etl_pipeline de_dagster 2>/dev/null || true
echo "✅ Đã dừng Dagster services"
echo ""

# 2. Restart MySQL với config mới
echo "2️⃣ Restart MySQL với config mới..."
docker-compose down de_mysql 2>/dev/null || true
docker-compose up -d de_mysql
echo "✅ Đã restart MySQL"
echo ""

# 3. Đợi MySQL healthy
echo "3️⃣ Đợi MySQL healthy..."
for i in {1..30}; do
  if docker-compose exec -T de_mysql mysqladmin ping -h localhost -uroot -padmin123 2>/dev/null | grep -q "alive"; then
    echo "✅ MySQL đã healthy"
    break
  fi
  echo "   Đợi... ($i/30)"
  sleep 2
done
echo ""

# 4. Verify MySQL config
echo "4️⃣ Kiểm tra MySQL config..."
docker-compose exec -T de_mysql mysql -uroot -padmin123 -e "
SHOW VARIABLES WHERE Variable_name IN ('max_connections','wait_timeout','innodb_buffer_pool_size','innodb_force_recovery');
" 2>/dev/null || echo "⚠️  Chưa thể verify (MySQL có thể chưa sẵn sàng)"
echo ""

# 5. Restart Hive Metastore
echo "5️⃣ Restart Hive Metastore..."
docker-compose restart hive-metastore
echo "✅ Đã restart Hive Metastore"
echo ""

# 6. Đợi HMS start
echo "6️⃣ Đợi Hive Metastore khởi động..."
sleep 30
echo ""

# 7. Restart Dagster services
echo "7️⃣ Restart Dagster services..."
docker-compose up -d de_dagster etl_pipeline de_dagster_dagit
echo "✅ Đã restart Dagster services"
echo ""

# 8. Chạy migrate
echo "8️⃣ Chạy Dagster instance migrate..."
docker-compose exec -T etl_pipeline dagster instance migrate 2>&1 | tail -5 || echo "⚠️  Migrate có thể đã được chạy"
echo ""

# 9. Verify
echo "9️⃣ Kiểm tra trạng thái..."
docker-compose ps | grep -E "de_mysql|de_dagster|etl_pipeline|hive-metastore"
echo ""

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     ✅ HOÀN TẤT                                               ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Kiểm tra:"
echo "   - MySQL: docker-compose exec de_mysql mysql -uroot -padmin123 -e 'SHOW VARIABLES LIKE \"max_connections\";'"
echo "   - Dagster UI: http://localhost:3001"
echo "   - Hive Metastore: docker-compose logs hive-metastore | tail -20"
echo ""

