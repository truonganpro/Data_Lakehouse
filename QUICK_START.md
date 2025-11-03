# 🚀 QUICK START GUIDE - Data Lakehouse Fresh

Hướng dẫn triển khai hoàn chỉnh từ đầu đến cuối trên máy mới.

---

## ✅ YÊU CẦU HỆ THỐNG

- **Docker** & **Docker Compose** đã cài đặt
- **8GB RAM** trở lên
- **20GB** dung lượng trống
- **Internet** để download JARs và Docker images

---

## 📋 BƯỚC 1: CLONE REPOSITORY

```bash
# Clone project từ GitHub
git clone https://github.com/truonganpro/Data_Warehouse.git

# Di chuyển vào thư mục project
cd Data_Warehouse

# Kiểm tra cấu trúc project
ls -la
```

---

## 📋 BƯỚC 2: DOWNLOAD JAR DEPENDENCIES

```bash
# Cấp quyền thực thi cho script
chmod +x download_jars.sh

# Download các JAR files cần thiết
./download_jars.sh
```

**Thời gian:** 1-2 phút

---

## 📋 BƯỚC 3: TẠO FILE .ENV

```bash
# Tạo file .env từ template
cp env.example .env

# Chỉnh sửa .env để thêm Google API Key (nếu cần Chat Service)
nano .env  # hoặc vi, vim, code, etc.

# Tìm dòng GOOGLE_API_KEY và thay YOUR_GOOGLE_API_KEY_HERE bằng API key thật
```

**Lưu ý:** Nếu chưa có Google API Key, bạn có thể bỏ qua bước này và thêm sau.

---

## 📋 BƯỚC 4: KHỞI ĐỘNG SERVICES

### Option A: Automated Setup (Recommended)

```bash
# Cấp quyền thực thi
chmod +x setup.sh

# Chạy script tự động setup toàn bộ
./setup.sh
```

**Thời gian:** 15-20 phút (lần đầu tiên)

Script sẽ tự động:
1. ✅ Kiểm tra Docker, disk space, RAM
2. ✅ Tạo .env nếu chưa có
3. ✅ Download JARs
4. ✅ Build Docker images
5. ✅ Khởi động tất cả services
6. ✅ Chạy ETL pipeline (nếu có data)
7. ✅ Kiểm tra health

### Option B: Manual Setup

```bash
# Build Docker images
docker-compose build

# Khởi động services
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

---

## 📋 BƯỚC 5: TẢI DATASET (Brazilian E-commerce)

**Nếu chưa có dataset:**

```bash
# Tạo thư mục cho dataset
mkdir -p brazilian-ecommerce

# Bạn cần download dataset từ Kaggle:
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# 
# Giải nén và đặt các file CSV vào thư mục brazilian-ecommerce/

# Sau đó chạy full_setup.sh để load data
chmod +x full_setup.sh
./full_setup.sh --fresh
```

**Nếu đã có dataset trong thư mục brazilian-ecommerce:**

```bash
# Load data vào MySQL và chạy ETL
chmod +x full_setup.sh
./full_setup.sh --fresh
```

---

## 📋 BƯỚC 6: KIỂM TRA SERVICES

```bash
# Kiểm tra tất cả services
docker-compose ps

# Xem logs của một service cụ thể
docker-compose logs -f streamlit
docker-compose logs -f chat_service
docker-compose logs -f etl_pipeline

# Kiểm tra health
curl http://localhost:8501/_stcore/health  # Streamlit
curl http://localhost:8001/health          # Chat Service
curl http://localhost:3000                 # Metabase
curl http://localhost:3001                 # Dagster
```

---

## 📋 BƯỚC 7: TRUY CẬP WEB INTERFACES

Sau khi tất cả services đã chạy (2-3 phút), mở trình duyệt:

| Service | URL | Ghi chú |
|---------|-----|---------|
| **🚀 Streamlit App** | http://localhost:8501 | Main dashboard |
| **💬 Chat Service** | http://localhost:8001 | API endpoint |
| **📊 Metabase** | http://localhost:3000 | BI Dashboard (cần setup) |
| **🎯 Dagster** | http://localhost:3001 | ETL Orchestration |
| **⚡ Spark Master** | http://localhost:8080 | Spark UI |
| **🪣 MinIO Console** | http://localhost:9001 | Object Storage |
| **🔍 Trino** | http://localhost:8082 | SQL Query Engine |

---

## 📋 BƯỚC 8: SETUP METABASE (Lần đầu)

1. Mở http://localhost:3000
2. Tạo admin account (email/password)
3. Add Database:
   - **Type:** Presto
   - **Host:** trino
   - **Port:** 8080
   - **Database:** lakehouse
4. Select schemas: `bronze`, `silver`, `gold`, `platinum`
5. Test connection → Save

---

## 📋 BƯỚC 9: CHẠY FORECASTING PIPELINE (Optional)

```bash
# Chạy toàn bộ forecasting pipeline
chmod +x run_forecast_pipeline.sh
./run_forecast_pipeline.sh

# Hoặc chạy từng bước với Makefile
make forecast-init      # Initialize MLflow
make forecast-features  # Build features
make forecast-train     # Train model
make forecast-predict RUN_ID=<run_id>  # Generate forecasts

# Kiểm tra kết quả trong Trino
docker exec trino trino --execute "SELECT * FROM lakehouse.platinum.demand_forecast LIMIT 10;"
```

---

## 📋 CÁC LỆNH QUAN TRỌNG

### Khởi động/Dừng services

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Stop và xóa volumes (cảnh báo: mất data)
docker-compose down -v

# Restart một service
docker-compose restart streamlit
```

### Xem logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f streamlit
docker-compose logs -f etl_pipeline
docker-compose logs -f chat_service

# Last 100 lines
docker-compose logs --tail=100 streamlit
```

### Rebuild sau khi sửa code

```bash
# Rebuild specific service
docker-compose build streamlit
docker-compose up -d streamlit

# Rebuild all
docker-compose build
docker-compose up -d
```

### Chạy ETL pipeline

```bash
# Chạy ETL job
docker exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data

# Hoặc dùng Makefile
make etl_bronze
```

### Kiểm tra data

```bash
# Kết nối Trino
docker exec -it trino trino

# Trong Trino CLI:
SHOW CATALOGS;
USE lakehouse.gold;
SHOW TABLES;
SELECT COUNT(*) FROM fact_order;

# Kiểm tra MinIO
docker exec mc mc ls minio/lakehouse/
```

---

## 🆘 TROUBLESHOOTING

### Services không khởi động được

```bash
# Kiểm tra Docker daemon
docker ps

# Kiểm tra disk space
df -h

# Xem logs chi tiết
docker-compose logs -f

# Clean và restart
docker-compose down -v
docker-compose up -d
```

### Port bị conflict

```bash
# Kiểm tra port đang dùng
lsof -i :8501
lsof -i :3000

# Dừng process chiếm port hoặc đổi port trong docker-compose.yaml
```

### Out of memory

```bash
# Kiểm tra RAM
free -h

# Reduce Spark memory trong docker-compose.yaml
# Giảm SPARK_WORKER_MEMORY từ 8G xuống 4G
```

### Dataset load failed

```bash
# Kiểm tra MySQL logs
docker-compose logs de_mysql

# Manual load
docker exec de_mysql mysql -uroot -padmin123 -e "SHOW DATABASES;"
docker cp brazilian-ecommerce/ de_mysql:/tmp/dataset/
```

---

## ✅ KIỂM TRA HOÀN TẤT

Sau khi setup xong, kiểm tra:

1. ✅ Tất cả services running: `docker-compose ps`
2. ✅ Streamlit accessible: http://localhost:8501
3. ✅ Chat Service healthy: `curl http://localhost:8001/health`
4. ✅ Data loaded: `SELECT COUNT(*) FROM lakehouse.gold.fact_order;`
5. ✅ Forecast working: `SELECT * FROM lakehouse.platinum.demand_forecast LIMIT 10;`

---

## 📚 TÀI LIỆU THAM KHẢO

- `README.md` - Tổng quan project
- `PROJECT_OVERVIEW.md` - Chi tiết kiến trúc
- `CHAT_SERVICE_FILES.txt` - Chat service docs
- `FORECAST_FILES.txt` - Forecast pipeline docs
- `QUERY_WINDOW_FILES.txt` - Query Window docs

---

## 🎉 HOÀN TẤT!

Nếu tất cả bước trên đều OK, project của bạn đã sẵn sàng sử dụng!

**Happy Data Engineering! 🚀**

