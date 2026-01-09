# Hướng dẫn sử dụng Spark Image trên máy khác

## Vấn đề
Image `bitnami/spark:3.3.2` có thể không còn available trên Docker Hub hoặc không thể pull trên máy khác.

## Giải pháp

### Cách 1: Export/Import Image (Khuyến nghị)

#### Trên máy hiện tại (đã có image):

1. **Export image:**
```bash
./export_spark_image.sh
```

Hoặc thủ công:
```bash
docker save bitnami/spark:3.3.2 -o spark-3.3.2-image.tar
```

2. **Nén file (tùy chọn, giảm kích thước):**
```bash
gzip spark-3.3.2-image.tar
```

3. **Copy file sang máy khác:**
   - Sử dụng USB, network share, hoặc cloud storage
   - File: `spark-3.3.2-image.tar` hoặc `spark-3.3.2-image.tar.gz` (~790MB)

#### Trên máy khác:

1. **Copy file vào thư mục dự án**

2. **Nếu file đã nén, giải nén trước:**
```bash
gunzip spark-3.3.2-image.tar.gz
```

3. **Import image:**
```bash
./import_spark_image.sh
```

Hoặc thủ công:
```bash
docker load -i spark-3.3.2-image.tar
```

4. **Kiểm tra:**
```bash
docker images bitnami/spark:3.3.2
```

5. **Chạy dự án:**
```bash
docker compose up -d
```

### Cách 2: Build từ Dockerfile (nếu base image còn available)

Nếu `bitnami/spark:3.3.2` vẫn có thể pull được:

```bash
cd docker_image/spark
docker build -t bitnami/spark:3.3.2 .
```

### Cách 3: Sử dụng Alternative Image (nếu không thể import)

Nếu không thể import image, có thể build từ Dockerfile alternative:

1. **Build custom Spark image:**
```bash
cd docker_image/spark
docker build -f Dockerfile.alternative -t spark-custom:3.3.2 .
```

2. **Chạy với docker-compose override:**
```bash
docker compose -f docker-compose.yaml -f docker-compose.spark-alternative.yaml up -d
```

Hoặc chỉnh sửa `docker-compose.yaml` để sử dụng image vừa build:
```yaml
spark-master:
  image: spark-custom:3.3.2
  # ... cập nhật paths từ /opt/bitnami/spark thành /opt/spark
```

## Files liên quan

- `spark-3.3.2-image.tar` - Image file (790MB)
- `spark-3.3.2-image.tar.gz` - Compressed version
- `export_spark_image.sh` - Script export
- `import_spark_image.sh` - Script import
- `docker_image/spark/Dockerfile` - Dockerfile để build lại

## Lưu ý

- File image khá lớn (~790MB), nên nén trước khi transfer
- Đảm bảo Docker đã được cài đặt trên máy đích
- Kiểm tra disk space đủ trước khi import
- File đã được thêm vào `.gitignore` để không commit vào git
