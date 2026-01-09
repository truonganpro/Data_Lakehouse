#!/bin/bash
# Script để import tất cả Docker images từ thư mục export

set -e

INPUT_DIR="docker_images_export"

if [ ! -d "$INPUT_DIR" ]; then
    echo "❌ Không tìm thấy thư mục $INPUT_DIR"
    echo "Vui lòng đảm bảo thư mục đã được copy vào đây"
    exit 1
fi

echo "============================================================================"
echo "📥 IMPORT TẤT CẢ DOCKER IMAGES"
echo "============================================================================"
echo ""

cd "$INPUT_DIR"

# Đếm số files
TAR_FILES=$(ls -1 *.tar 2>/dev/null | wc -l | tr -d ' ')
GZ_FILES=$(ls -1 *.tar.gz 2>/dev/null | wc -l | tr -d ' ')
TOTAL=$((TAR_FILES + GZ_FILES))

if [ "$TOTAL" -eq 0 ]; then
    echo "❌ Không tìm thấy file image nào trong $INPUT_DIR"
    exit 1
fi

echo "Tìm thấy $TOTAL image files"
echo ""

# Import các file .tar.gz trước (giải nén tự động)
if [ "$GZ_FILES" -gt 0 ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Importing compressed images (.tar.gz)..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    for file in *.tar.gz; do
        if [ -f "$file" ]; then
            echo "Importing: $file"
            gunzip -c "$file" | docker load
            echo "  ✅ Done"
        fi
    done
fi

# Import các file .tar
if [ "$TAR_FILES" -gt 0 ]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Importing uncompressed images (.tar)..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    for file in *.tar; do
        if [ -f "$file" ]; then
            echo "Importing: $file"
            docker load -i "$file"
            echo "  ✅ Done"
        fi
    done
fi

cd ..

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ HOÀN TẤT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Kiểm tra images đã import:"
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}" | head -20
echo ""
echo "💡 Bây giờ bạn có thể chạy: docker compose up -d"
