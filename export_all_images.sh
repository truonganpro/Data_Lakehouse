#!/bin/bash
# Script để export tất cả Docker images cần thiết cho dự án

set -e

OUTPUT_DIR="docker_images_export"
mkdir -p "$OUTPUT_DIR"

echo "============================================================================"
echo "📦 EXPORT TẤT CẢ DOCKER IMAGES CHO DỰ ÁN"
echo "============================================================================"
echo ""

# Danh sách images từ Docker Hub (có thể pull lại nhưng export để đảm bảo)
EXTERNAL_IMAGES=(
    "mysql:8.0"
    "trinodb/trino:414"
    "bitnami/spark:3.3.2"
    "minio/minio:latest"
    "minio/mc:latest"
    "metabase/metabase:latest"
    "qdrant/qdrant:latest"
)

# Danh sách images được build từ dự án (quan trọng nhất)
PROJECT_IMAGES=(
    "data_warehouse_fresh-chat_service:latest"
    "data_warehouse_fresh-de_dagster:latest"
    "data_warehouse_fresh-de_dagster_daemon:latest"
    "data_warehouse_fresh-de_dagster_dagit:latest"
    "data_warehouse_fresh-etl_pipeline:latest"
    "data_warehouse_fresh-hive-metastore:latest"
    "streamlit:latest"
)

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1. Export External Images (từ Docker Hub)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

for img in "${EXTERNAL_IMAGES[@]}"; do
    if docker images "$img" --format "{{.Repository}}:{{.Tag}}" 2>/dev/null | grep -q "$img"; then
        FILENAME=$(echo "$img" | tr '/:' '_')
        echo "Exporting: $img -> $OUTPUT_DIR/${FILENAME}.tar"
        docker save "$img" -o "$OUTPUT_DIR/${FILENAME}.tar"
        echo "  ✅ Done ($(ls -lh "$OUTPUT_DIR/${FILENAME}.tar" | awk '{print $5}'))"
    else
        echo "  ⚠️  Image không tồn tại: $img"
    fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2. Export Project Images (được build từ dự án)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

for img in "${PROJECT_IMAGES[@]}"; do
    if docker images "$img" --format "{{.Repository}}:{{.Tag}}" 2>/dev/null | grep -q "$img"; then
        FILENAME=$(echo "$img" | tr '/:' '_')
        echo "Exporting: $img -> $OUTPUT_DIR/${FILENAME}.tar"
        docker save "$img" -o "$OUTPUT_DIR/${FILENAME}.tar"
        echo "  ✅ Done ($(ls -lh "$OUTPUT_DIR/${FILENAME}.tar" | awk '{print $5}'))"
    else
        echo "  ⚠️  Image không tồn tại: $img"
    fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3. Nén tất cả files (tùy chọn)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

read -p "Bạn có muốn nén tất cả files? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Đang nén..."
    cd "$OUTPUT_DIR"
    for file in *.tar; do
        if [ -f "$file" ]; then
            echo "  Nén: $file"
            gzip "$file"
        fi
    done
    cd ..
    echo "✅ Đã nén xong!"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 TÓM TẮT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✅ Tất cả images đã được export vào: $OUTPUT_DIR/"
echo ""
TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" | awk '{print $1}')
echo "📦 Tổng dung lượng: $TOTAL_SIZE"
echo ""
echo "💡 Để import trên máy khác:"
echo "   ./import_all_images.sh"
echo "   hoặc: cd $OUTPUT_DIR && for f in *.tar*; do docker load -i \"\$f\"; done"
