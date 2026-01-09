#!/bin/bash
# Script để import Spark image trên máy khác

set -e

IMAGE_FILE="spark-3.3.2-image.tar"
IMAGE_NAME="bitnami/spark:3.3.2"

echo "Đang import Spark image..."
if [ ! -f "$IMAGE_FILE" ]; then
    echo "❌ Không tìm thấy file $IMAGE_FILE"
    echo "Vui lòng đảm bảo file đã được copy vào thư mục này"
    exit 1
fi

echo "Importing $IMAGE_NAME từ $IMAGE_FILE..."
docker load -i "$IMAGE_FILE"

echo "✅ Đã import thành công!"
echo "Kiểm tra image:"
docker images "$IMAGE_NAME"
