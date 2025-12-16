# Data Lakehouse - Modern Data Stack

Hệ thống Data Lakehouse hoàn chỉnh với ETL Pipeline, OLAP Query Engine, AI Chatbot, và BI Dashboards.

## 🏗️ Kiến trúc

```
┌─────────────────────────────────────────────────────────┐
│              USER INTERFACES                            │
├──────────┬──────────┬──────────┬───────────────────────┤
│ Streamlit│ Metabase │  Dagster │   Chat Service        │
│  :8501   │  :3000   │  :3001   │      :8001            │
└──────────┴──────────┴──────────┴───────────────────────┘
         ↓           ↓           ↓              ↓
┌─────────────────────────────────────────────────────────┐
│         QUERY & PROCESSING LAYER                        │
├──────────┬──────────┬──────────┬───────────────────────┤
│   Trino  │  Spark   │  MLflow  │   Chat Service        │
│  :8082   │  :8080   │  :5000   │      :8001            │
└──────────┴──────────┴──────────┴───────────────────────┘
         ↓           ↓           ↓              ↓
┌─────────────────────────────────────────────────────────┐
│         STORAGE & METADATA LAYER                        │
├──────────┬──────────┬──────────┬───────────────────────┤
│Delta Lake│   MinIO   │   MySQL  │      Qdrant          │
│(Lakehouse)│ (S3)    │(Metadata)│   (Vector DB)        │
└──────────┴──────────┴──────────┴───────────────────────┘
```

## 📋 Yêu cầu hệ thống

- **Docker**: 20.10+ và Docker Compose 2.0+
- **Disk Space**: Tối thiểu 20GB trống
- **RAM**: Tối thiểu 8GB (khuyến nghị 16GB)
- **OS**: Linux, macOS, hoặc Windows với WSL2

## 🚀 Quick Start

### 1. Clone repository

```bash
git clone <repository-url>
cd Data_lakehouse
```

### 2. Download dataset (nếu chưa có)

Dataset Brazilian E-commerce từ Kaggle:
- URL: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Đặt vào thư mục `brazilian-ecommerce/`

### 3. Chạy setup tự động

```bash
# Quick setup (khuyến nghị cho lần đầu)
./setup.sh

# Hoặc full setup với tùy chọn
./full_setup.sh --fresh
```

### 4. Truy cập các services

Sau khi setup hoàn tất, truy cập:

- **Streamlit Dashboard**: http://localhost:8501
- **Dagster UI**: http://localhost:3001
- **Metabase BI**: http://localhost:3000
- **Trino UI**: http://localhost:8082
- **Spark Master**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (minio/minio123)
- **Chat Service API**: http://localhost:8001

## 📦 Cấu trúc dự án

```
Data_lakehouse/
├── app/                    # Streamlit UI application
│   ├── app.py             # Main dashboard
│   └── pages/             # Streamlit pages
├── chat_service/          # FastAPI Chat Service
│   ├── main.py           # API endpoints
│   ├── skills/           # SQL generation skills
│   └── llm/              # LLM integration
├── etl_pipeline/         # Spark ETL jobs
│   └── etl_pipeline/     # Dagster assets & jobs
├── dagster/              # Dagster configuration
├── docker-compose.yaml   # Service orchestration
├── setup.sh              # Quick setup script
├── full_setup.sh         # Full setup script
└── env.example           # Environment template
```

## 🔧 Cấu hình

### Environment Variables

Copy `env.example` thành `.env` và cấu hình:

```bash
cp env.example .env
```

**Quan trọng**: Cập nhật `GOOGLE_API_KEY` trong `.env` để sử dụng Chat Service:

```env
GOOGLE_API_KEY=your_google_api_key_here
```

### Các biến môi trường chính:

- `MYSQL_ROOT_PASSWORD`: MySQL root password
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`: MinIO credentials
- `GOOGLE_API_KEY`: Google Gemini API key (cho Chat Service)
- `LLM_API_KEY`: OpenAI API key (tùy chọn)

## 📊 Data Pipeline

### Medallion Architecture

1. **Bronze Layer**: Raw data từ MySQL
2. **Silver Layer**: Cleaned & validated data
3. **Gold Layer**: Star schema (fact & dimension tables)
4. **Platinum Layer**: Business datamarts

### Chạy ETL

```bash
# Chạy ETL pipeline
./full_setup.sh --etl

# Hoặc qua Dagster UI
# Mở http://localhost:3001 và chạy job "reload_data"
```

### Tối ưu hóa Lakehouse (Compaction & Vacuum)

Hệ thống tự động chạy **OPTIMIZE** và **VACUUM** cho các bảng Delta Lake mỗi ngày lúc 3:00 AM (sau khi ETL hoàn thành).

#### Chạy thủ công

```bash
# Chạy optimize script trực tiếp
docker exec -it spark-master bash /scripts/optimize_lakehouse.sh

# Hoặc qua Dagster UI
# Mở http://localhost:3001 và chạy job "optimize_lakehouse_job"
```

#### Tự động hóa

- **Schedule**: Chạy tự động mỗi ngày lúc 3:00 AM (sau ETL)
- **Lớp được tối ưu**: Gold và Platinum
- **Retention**: 168 giờ (7 ngày) - đúng chuẩn Delta Lake
- **Chức năng**:
  - **OPTIMIZE**: Gom các file nhỏ thành file lớn hơn (compaction)
  - **VACUUM**: Xóa các file cũ không còn cần thiết

#### Bảng được tối ưu

**Gold Layer:**
- `fact_order`
- `fact_order_item`
- `dim_customer`
- `dim_product`
- `dim_seller`

**Platinum Layer:**
- `dm_sales_monthly_category`
- `dm_seller_kpi`
- `dm_customer_lifecycle`
- `dm_payment_mix`
- `dm_logistics_sla`
- `dm_product_bestsellers`
- `dm_category_price_bands`
- `forecast_monitoring`

## 🛠️ Các lệnh hữu ích

### Docker Compose

```bash
# Xem trạng thái services
docker compose ps

# Xem logs
docker compose logs -f <service_name>

# Restart service
docker compose restart <service_name>

# Stop tất cả
docker compose down

# Stop và xóa volumes
docker compose down -v
```

### Makefile

```bash
# Build tất cả
make build

# Start services
make up

# Stop services
make down

# Rebuild và restart
make rebuild
```

## 🐛 Troubleshooting

### Services không start

1. Kiểm tra logs: `docker compose logs <service_name>`
2. Kiểm tra disk space: `df -h`
3. Kiểm tra ports đang được sử dụng: `lsof -i :PORT`

### ETL pipeline fails

1. Kiểm tra MySQL connection: `docker exec de_mysql mysql -uroot -padmin123 -e "SHOW DATABASES;"`
2. Kiểm tra Spark: http://localhost:8080
3. Xem logs ETL: `docker compose logs etl_pipeline`

### Chat Service không hoạt động

1. Kiểm tra GOOGLE_API_KEY trong `.env`
2. Test API: `curl http://localhost:8001/healthz`
3. Xem logs: `docker compose logs chat_service`

## 📚 Tài liệu

- [Chat Service Guide](docs/chat_service_guide.md)
- [Data Dictionary](docs/data_dictionary.md)
- [KPI Definitions](docs/kpi_definitions.md)
- [Forecast Documentation](docs/forecast/README_forecast.md)

## 🔒 Bảo mật

- **Không commit** file `.env` vào git
- Đổi passwords mặc định trong production
- Sử dụng environment variables cho API keys
- Giới hạn network access trong docker-compose

## 📝 License

MIT License

## 👤 Author

**Truong An**

- Project Creator & Developer
- Data Lakehouse - Modern Data Stack
- License: MIT

> Thông tin tác giả cũng có thể được tìm thấy trong:
> - `AUTHORS.md` - Chi tiết về tác giả
> - `app/app.py` - Footer của Streamlit dashboard
> - Git commit history (nếu có)

---

**Happy Data Engineering! 🚀**

