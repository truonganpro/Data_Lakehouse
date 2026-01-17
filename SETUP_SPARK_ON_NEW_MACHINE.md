# Hướng dẫn Setup Spark Image trên máy mới

## Trên máy hiện tại (đã có Spark image)

### Bước 1: Export Spark image
```bash
./export_spark_image.sh
```

Hoặc thủ công:
```bash
docker save bitnami/spark:3.3.2 -o spark-3.3.2-image.tar
```

### Bước 2: Nén file (tùy chọn)
```bash
gzip spark-3.3.2-image.tar
```

### Bước 3: Copy file sang máy mới
- File: `spark-3.3.2-image.tar` hoặc `spark-3.3.2-image.tar.gz` (~790MB)
- Copy qua USB, network, hoặc cloud storage

## Trên máy mới

### Bước 1: Clone dự án từ Git
```bash
git clone <repository-url>
cd Data_Warehouse_Fresh
```

### Bước 2: Copy file Spark image vào thư mục dự án
```bash
# Copy file spark-3.3.2-image.tar hoặc .tar.gz vào đây
```

### Bước 3: Nếu file đã nén, giải nén
```bash
gunzip spark-3.3.2-image.tar.gz
```

### Bước 4: Import Spark image
```bash
./import_spark_image.sh
```

Hoặc thủ công:
```bash
docker load -i spark-3.3.2-image.tar
```

### Bước 5: Kiểm tra image đã import
```bash
docker images bitnami/spark:3.3.2
```

### Bước 6: Setup môi trường
```bash
# Copy env.example thành .env
cp env.example .env

# Cập nhật GOOGLE_API_KEY trong .env (nếu cần)
nano .env
```

### Bước 7: Chạy dự án
```bash
docker compose up -d
```

### Bước 8: Kiểm tra services
```bash
docker compose ps
```

## Lưu ý

- File `spark-3.3.2-image.tar` đã được ignore trong `.gitignore`
- Các images khác sẽ được pull tự động từ Docker Hub khi chạy `docker compose up`
- Nếu không có file Spark image, có thể dùng alternative Dockerfile (xem `SPARK_IMAGE_SETUP.md`)
