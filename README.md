# 🏗️ Modern Data Stack - Data Lakehouse Project

> **Ứng dụng Modern Data Stack (MDS) để xây dựng Data Lakehouse hỗ trợ phân tích dữ liệu bán hàng thương mại điện tử**

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Trino](https://img.shields.io/badge/Trino-FF6900?style=for-the-badge&logo=trino&logoColor=white)](https://trino.io/)
[![Dagster](https://img.shields.io/badge/Dagster-000000?style=for-the-badge&logo=dagster&logoColor=white)](https://dagster.io/)
[![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=for-the-badge&logo=metabase&logoColor=white)](https://www.metabase.com/)

---

## 📋 Mục lục

- [🎯 Tổng quan dự án](#-tổng-quan-dự-án)
- [🏗️ Kiến trúc hệ thống](#️-kiến-trúc-hệ-thống)
- [🛠️ Công nghệ sử dụng](#️-công-nghệ-sử-dụng)
- [🚀 Cài đặt và chạy](#-cài-đặt-và-chạy)
- [📊 Dataset](#-dataset)
- [📈 Data Layers](#-data-layers)
- [🎨 Dashboard và BI](#-dashboard-và-bi)
- [🧪 Testing](#-testing)
- [📚 Tài liệu tham khảo](#-tài-liệu-tham-khảo)

---

## 🎯 Tổng quan dự án

### Mục tiêu

Xây dựng một hệ thống **Data Lakehouse** hoàn chỉnh sử dụng **Modern Data Stack** để:

- ✅ **Thu thập và xử lý** dữ liệu thương mại điện tử Brazilian E-commerce
- ✅ **Tổ chức dữ liệu** theo mô hình **Medallion Architecture** (Bronze → Silver → Gold → Platinum)
- ✅ **Phân tích và trực quan hóa** dữ liệu qua BI dashboard
- ✅ **Triển khai container hóa** với Docker Compose

---

## 🏗️ Kiến trúc hệ thống

```
MySQL (Brazilian E-commerce Data)
    ↓
Bronze Layer → MinIO (lakehouse/bronze/)
    ↓
Silver Layer → MinIO (lakehouse/silver/)
    ↓
Gold Layer → MinIO (lakehouse/gold/)
    ↓
Platinum Layer → MinIO (lakehouse/platinum/)
    ↓
Trino (catalog: minio, schema: platinum)
    ↓
Metabase (http://localhost:3000)
```

### Medallion Architecture

| Layer | Mô tả | Số bảng |
|-------|-------|---------|
| **Bronze** | Raw data từ MySQL | 9 tables |
| **Silver** | Cleaned & normalized data | 10 tables |
| **Gold** | Star schema (Facts + Dimensions) | 10 tables |
| **Platinum** | Business datamarts cho BI | 7 datamarts |

---

## 🛠️ Công nghệ sử dụng

| **Thành phần** | **Công nghệ** | **Vai trò** |
|----------------|---------------|-------------|
| **Storage** | MinIO (S3-compatible) | Lưu trữ dữ liệu theo zone |
| **Metadata** | Hive Metastore + MySQL | Quản lý schema và metadata |
| **Compute** | Apache Spark 3.3.2 | Xử lý dữ liệu (ETL) |
| **Lakehouse** | Delta Lake 2.3.0 | ACID transactions, versioning |
| **Query Engine** | Trino 414 | SQL query trên Delta tables |
| **Orchestration** | Dagster | Workflow management |
| **BI/Visualization** | Metabase + Streamlit | Dashboard và báo cáo |
| **Containerization** | Docker Compose | Triển khai và quản lý |

---

## 🚀 Cài đặt và chạy

### Yêu cầu hệ thống

- Docker & Docker Compose
- 8GB RAM trở lên
- 20GB dung lượng trống

### Quick Start

```bash
# 1. Clone repository
git clone <repository-url>
cd Data_Warehouse_Fresh

# 2. Download JAR dependencies
chmod +x download_jars.sh
./download_jars.sh

# 3. Khởi động hệ thống (tự động tạo .env nếu chưa có)
chmod +x start_and_test.sh
./start_and_test.sh

# Hoặc sử dụng docker-compose trực tiếp
docker-compose up -d
```

### Kiểm tra trạng thái

```bash
# Kiểm tra containers
docker-compose ps

# Xem logs
docker-compose logs -f [service_name]
```

### Truy cập các giao diện

| Service | URL | Credentials |
|---------|-----|-------------|
| **Spark Master UI** | http://localhost:8080 | - |
| **MinIO Console** | http://localhost:9001 | minio/minio123 |
| **Metabase** | http://localhost:3000 | Setup on first access |
| **Trino** | http://localhost:8082 | - |
| **Dagster** | http://localhost:3001 | - |
| **Streamlit** | http://localhost:8501 | - |
| **Jupyter Notebook** | http://localhost:8888 | - |

---

## 📊 Dataset

### Brazilian E-commerce Dataset

- **Nguồn**: [Kaggle - Brazilian E-commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Kích thước**: ~100K đơn hàng, 32K sản phẩm, 9K người bán
- **Thời gian**: 2016-2018

### Download Dataset

```bash
# Tạo thư mục dataset
mkdir -p brazilian-ecommerce

# Download từ Kaggle (cần Kaggle API)
kaggle datasets download -d olistbr/brazilian-ecommerce -p brazilian-ecommerce/
unzip brazilian-ecommerce/brazilian-ecommerce.zip -d brazilian-ecommerce/
```

### Schema

| Table | Description | Key Fields |
|-------|-------------|------------|
| `customers` | Customer information | customer_id, customer_zip_code_prefix |
| `orders` | Order details | order_id, customer_id, order_status |
| `order_items` | Order line items | order_id, product_id, seller_id, price |
| `products` | Product catalog | product_id, product_category_name |
| `sellers` | Seller information | seller_id, seller_zip_code_prefix |
| `geolocation` | Geographic data | geolocation_zip_code_prefix, city, state |
| `order_payments` | Payment information | order_id, payment_type, payment_value |
| `order_reviews` | Customer reviews | review_id, order_id, review_score |

---

## 📈 Data Layers

### Platinum Layer Datamarts

| Datamart | Records | Mô Tả |
|----------|---------|-------|
| `dm_sales_monthly_category` | 1,326 | Doanh số theo tháng và danh mục sản phẩm |
| `dm_seller_kpi` | 3,095 | KPI và hiệu suất của người bán |
| `dm_customer_lifecycle` | 96,462 | Vòng đời và hành vi khách hàng |
| `dm_payment_mix` | 90 | Phân tích phương thức thanh toán |
| `dm_logistics_sla` | 574 | SLA và hiệu suất giao hàng |
| `dm_product_bestsellers` | 32,951 | Sản phẩm bán chạy nhất |
| `dm_category_price_bands` | 326 | Phân khúc giá theo danh mục |
| **TỔNG** | **134,824** | **Tổng records trong Platinum Layer** |

### ETL Pipeline

1. **Bronze Layer**: Extract raw data từ MySQL → MinIO (Delta format)
2. **Silver Layer**: Data cleaning, normalization, type casting
3. **Gold Layer**: Star schema transformation (Facts + Dimensions)
4. **Platinum Layer**: Business aggregations và datamarts cho BI

---

## 🎨 Dashboard và BI

### Metabase Setup

1. Truy cập Metabase: http://localhost:3000
2. Setup admin account (lần đầu tiên)
3. Add Database:
   - Database Type: **Trino**
   - Host: `trino`
   - Port: `8080`
   - JDBC String: `catalog=minio&schema=platinum`
   - Username: `metabase`
   - SSL: **OFF**

### Sample Queries

#### Monthly Revenue Trend
```sql
SELECT 
    year_month,
    SUM(gmv) as total_revenue,
    SUM(orders) as total_orders,
    ROUND(SUM(gmv) / SUM(orders), 2) as aov
FROM minio.platinum.dm_sales_monthly_category
GROUP BY year_month
ORDER BY year_month
```

#### Top 10 Categories
```sql
SELECT 
    product_category_name_english as category,
    SUM(gmv) as revenue,
    SUM(orders) as orders,
    SUM(units) as units
FROM mino.platinum.dm_sales_monthly_category
WHERE product_category_name_english IS NOT NULL
GROUP BY product_category_name_english
ORDER BY revenue DESC
LIMIT 10
```

#### Payment Mix
```sql
SELECT 
    payment_type,
    SUM(orders) as total_orders,
    SUM(payment_total) as total_value,
    ROUND(SUM(orders) * 100.0 / SUM(SUM(orders)) OVER(), 2) as pct_orders
FROM minio.platinum.dm_payment_mix
WHERE payment_type IS NOT NULL
GROUP BY payment_type
ORDER BY total_orders DESC
```

### Recommended Dashboards

1. **Executive Summary**: Total GMV, Orders, AOV, Revenue trend
2. **Sales Deep Dive**: Category performance, Geographic distribution
3. **Customer Analytics**: Lifecycle funnel, Segmentation, Cohort analysis
4. **Operations**: Logistics SLA, Delivery metrics, Payment distribution
5. **Product Intelligence**: Bestsellers, Price bands, Category analysis

Xem chi tiết trong file `METABASE_SETUP_GUIDE.md` và `VISUALIZATION_RESULTS.md`.

---

## 🧪 Testing

### Test Spark ↔ MinIO Connection

```bash
python test_spark_minio_connection.py

# Hoặc qua Docker
docker exec spark-master python3 /opt/bitnami/spark/test_spark_minio_connection.py
```

### Test Trino Connection

```bash
chmod +x test_trino_connection.sh
./test_trino_connection.sh
```

### Test SQL Queries

```sql
-- Qua Trino CLI
docker exec trino trino

-- Trong Trino CLI:
SHOW CATALOGS;
USE minio.platinum;
SHOW TABLES;
SELECT * FROM dm_sales_monthly_category LIMIT 10;
```

---

## 🔧 Configuration

### Environment Variables

File `.env` được tạo tự động từ `env.example` khi khởi động. Các biến quan trọng:

```bash
# MySQL
MYSQL_ROOT_PASSWORD=root123
MYSQL_DATABASE=metastore
MYSQL_USER=hive
MYSQL_PASSWORD=hive

# Dagster MySQL
DAGSTER_MYSQL_HOSTNAME=de_mysql
DAGSTER_MYSQL_DB=dagster
DAGSTER_MYSQL_USERNAME=dagster
DAGSTER_MYSQL_PASSWORD=dagster123

# MinIO
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
```

### Spark Configuration

Key settings trong `docker_image/spark/conf/spark-defaults.conf`:

```properties
# Delta Lake
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# S3/MinIO
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=minio
spark.hadoop.fs.s3a.secret.key=minio123
spark.hadoop.fs.s3a.path.style.access=true

# Warehouse
spark.sql.warehouse.dir=s3a://lakehouse/
```

### Trino Configuration

Catalog `minio.properties`:

```properties
connector.name=delta_lake
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minio
hive.s3.aws-secret-key=minio123
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
delta.register-table-procedure.enabled=true
```

---

## 🔄 Dagster Jobs

### Run ETL Pipeline

```bash
# Access Dagster UI
http://localhost:3001

# Materialize assets by layer:
# - Bronze Layer: Extract from MySQL
# - Silver Layer: Clean and normalize
# - Gold Layer: Create star schema
# - Platinum Layer: Create datamarts

# Hoặc run via CLI
docker exec de_dagster_daemon dagster job execute -j full_pipeline_job
```

---

## 📚 Tài liệu tham khảo

### Công nghệ chính

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Trino Documentation](https://trino.io/docs/)
- [Dagster Documentation](https://docs.dagster.io/)
- [Metabase Documentation](https://www.metabase.com/docs/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

### Modern Data Stack

- [What is a Data Lakehouse?](https://www.databricks.com/discover/data-lakehouse)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Data Engineering Best Practices](https://github.com/datastacktv/data-engineer-roadmap)

### Dataset

- [Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## 🐛 Troubleshooting

### Services không khởi động

```bash
# Kiểm tra logs
docker-compose logs [service_name]

# Restart services
docker-compose restart [service_name]

# Rebuild nếu cần
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### MinIO connection errors

```bash
# Kiểm tra MinIO
docker exec minio mc ls minio/

# Test từ Spark
docker exec spark-master python3 /opt/bitnami/spark/test_spark_minio_connection.py
```

### Dagster job failures

```bash
# Check Dagster logs
docker-compose logs de_dagster_daemon

# Check ETL pipeline logs
docker-compose logs etl_pipeline
```

---

## 🤝 Đóng góp

1. Fork repository
2. Tạo feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Tạo Pull Request

---

## 📄 License

Distributed under the MIT License.

---

## 👨‍💻 Tác giả

**Truong An** - *Modern Data Stack Engineer*

---

## 🙏 Acknowledgments

- [Olist](https://olist.com/) for providing the Brazilian E-commerce dataset
- [Apache Software Foundation](https://apache.org/) for open-source tools
- [Modern Data Stack community](https://github.com/modern-data-stack) for inspiration

---

⭐ **Nếu dự án này hữu ích, hãy cho một star!** ⭐
