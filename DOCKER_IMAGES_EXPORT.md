# Hướng dẫn Export/Import Docker Images

## Tổng quan

Script này giúp export tất cả Docker images cần thiết cho dự án để có thể chạy trên máy khác mà không cần pull từ Docker Hub.

## Export Images

### Trên máy hiện tại (đã có tất cả images):

```bash
./export_all_images.sh
```

Script sẽ:
1. Export tất cả external images (từ Docker Hub)
2. Export tất cả project images (được build từ dự án)
3. Hỏi có muốn nén files không (khuyến nghị: Yes)

**Output:** Thư mục `docker_images_export/` chứa tất cả các file `.tar` hoặc `.tar.gz`

### Danh sách images được export:

**External Images:**
- `mysql:8.0` (~217MB)
- `trinodb/trino:414` (~941MB)
- `bitnami/spark:3.3.2` (~790MB)
- `minio/minio:latest` (~55MB)
- `minio/mc:latest` (~26MB)
- `metabase/metabase:latest` (~632MB)
- `qdrant/qdrant:latest` (~62MB)

**Project Images:**
- `data_warehouse_fresh-chat_service:latest` (~422MB)
- `data_warehouse_fresh-de_dagster:latest` (~204MB)
- `data_warehouse_fresh-de_dagster_daemon:latest` (~204MB)
- `data_warehouse_fresh-de_dagster_dagit:latest` (~204MB)
- `data_warehouse_fresh-etl_pipeline:latest` (~809MB)
- `data_warehouse_fresh-hive-metastore:latest` (~1.8GB)
- `streamlit:latest` (~402MB)

**Tổng dung lượng:** ~6.5GB (chưa nén) / ~5-6GB (sau khi nén)

## Import Images

### Trên máy khác:

1. **Copy thư mục `docker_images_export/` vào thư mục dự án**

2. **Import tất cả images:**
```bash
./import_all_images.sh
```

Hoặc thủ công:
```bash
cd docker_images_export
for f in *.tar.gz; do gunzip -c "$f" | docker load; done
for f in *.tar; do docker load -i "$f"; done
```

3. **Kiểm tra:**
```bash
docker images
```

4. **Chạy dự án:**
```bash
docker compose up -d
```

## Transfer Files

### Cách 1: USB/External Drive
- Copy toàn bộ thư mục `docker_images_export/` vào USB
- Copy sang máy đích

### Cách 2: Network Share
```bash
# Trên máy nguồn
scp -r docker_images_export/ user@target-machine:/path/to/project/

# Hoặc sử dụng rsync
rsync -avz docker_images_export/ user@target-machine:/path/to/project/docker_images_export/
```

### Cách 3: Cloud Storage
- Upload thư mục `docker_images_export/` lên Google Drive, Dropbox, etc.
- Download trên máy đích

## Lưu ý

- File images khá lớn (~6.5GB), nên nén trước khi transfer
- Đảm bảo có đủ disk space trên máy đích (ít nhất 10GB)
- Docker phải được cài đặt trên máy đích
- Thư mục `docker_images_export/` đã được thêm vào `.gitignore`

## Troubleshooting

### Nếu thiếu image:
```bash
# Kiểm tra image nào thiếu
docker compose config --images

# Export image cụ thể
docker save <image:tag> -o docker_images_export/<image_name>.tar
```

### Nếu import lỗi:
```bash
# Kiểm tra file có bị corrupt không
file docker_images_export/*.tar

# Thử import từng file một
docker load -i docker_images_export/<file>.tar
```
