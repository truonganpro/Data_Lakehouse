#!/bin/bash
# Script để export Spark image

set -e

IMAGE_NAME="bitnami/spark:3.3.2"
OUTPUT_FILE="spark-3.3.2-image.tar"

echo "Đang export Spark image..."
if ! docker images "$IMAGE_NAME" --format "{{.Repository}}:{{.Tag}}" | grep -q "$IMAGE_NAME"; then
    echo "❌ Không tìm thấy image $IMAGE_NAME"
    echo "Vui lòng pull hoặc build image trước"
    exit 1
fi

echo "Exporting $IMAGE_NAME to $OUTPUT_FILE..."
docker save "$IMAGE_NAME" -o "$OUTPUT_FILE"

echo "✅ Đã export thành công!"
echo "File: $OUTPUT_FILE"
ls -lh "$OUTPUT_FILE"

echo ""
echo "💡 Để nén file (giảm kích thước):"
echo "   gzip spark-3.3.2-image.tar"
echo ""
echo "💡 Để import trên máy khác:"
echo "   ./import_spark_image.sh"
echo "   hoặc: docker load -i spark-3.3.2-image.tar"
