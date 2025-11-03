# 🏗️ DATA LAKEHOUSE - MODERN DATA STACK PROJECT

## 📌 Giới thiệu

Dự án **Data Lakehouse** với **Modern Data Stack** - Một hệ thống phân tích dữ liệu hoàn chỉnh cho thương mại điện tử, tích hợp Machine Learning, OLAP Analytics, và Natural Language Query Interface.

---

## 🎯 Tổng quan dự án

### Mục tiêu chính

Xây dựng một hệ thống Data Lakehouse production-ready với khả năng:

✅ **ETL Pipeline tự động** - Medallion Architecture (Bronze → Silver → Gold → Platinum)  
✅ **Demand Forecasting** - Dự báo nhu cầu sản phẩm 28 ngày với LightGBM + MLflow  
✅ **OLAP Query Interface** - Truy vấn đa chiều với Streamlit + Trino  
✅ **AI-Powered Chat** - Hỏi đáp bằng ngôn ngữ tự nhiên với SQL + RAG  
✅ **BI Dashboard** - Trực quan hóa với Metabase  
✅ **Containerized Deployment** - Docker Compose với 12+ services  

---

## 🏗️ Kiến trúc hệ thống

### Tổng quan kiến trúc

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          USER INTERFACES                                 │
├──────────────┬──────────────┬──────────────┬─────────────────────────────┤
│  Streamlit   │  Metabase    │   Dagster    │   Jupyter                   │
│  (Port 8501) │  (Port 3000) │  (Port 3001) │   (Port 8888)               │
└──────────────┴──────────────┴──────────────┴─────────────────────────────┘
        ↓              ↓              ↓                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                       QUERY & PROCESSING LAYER                           │
├──────────────┬──────────────┬──────────────┬─────────────────────────────┤
│    Trino     │    Spark     │   MLflow     │   Chat Service              │
│  (Port 8082) │  (Port 8080) │  (Port 5000) │   (Port 8001)               │
└──────────────┴──────────────┴──────────────┴─────────────────────────────┘
        ↓              ↓              ↓                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      STORAGE & METADATA LAYER                            │
├──────────────┬──────────────┬──────────────┬─────────────────────────────┤
│  Delta Lake  │     MinIO    │    MySQL     │   Qdrant (Vector DB)        │
│  (Lakehouse) │  (S3 Object) │ (Metadata)   │   (RAG Embeddings)          │
└──────────────┴──────────────┴──────────────┴─────────────────────────────┘
```

### Medallion Architecture (Data Layers)

```
MySQL (Source)
    ↓
┌──────────────┐
│ Bronze Layer │ → Raw data từ MySQL (9 tables)
└──────────────┘
    ↓
┌──────────────┐
│ Silver Layer │ → Cleaned & normalized (10 tables + forecast_features)
└──────────────┘
    ↓
┌──────────────┐
│  Gold Layer  │ → Star schema: Facts + Dimensions (10 tables)
└──────────────┘
    ↓
┌──────────────┐
│Platinum Layer│ → Business datamarts (7 tables + demand_forecast)
└──────────────┘
```

---

## 🚀 Các tính năng chính

### 1️⃣ ETL Pipeline với Dagster

**Mô tả**: Pipeline tự động hóa với orchestration, scheduling, và monitoring

**Công nghệ**:
- Dagster (Orchestration)
- Apache Spark (Processing)
- Delta Lake (Storage)
- Hive Metastore (Catalog)

**Features**:
- ✅ Incremental loading
- ✅ Schema evolution
- ✅ Data quality checks
- ✅ Lineage tracking
- ✅ Job scheduling
- ✅ Error handling & retry

**Jobs**:
- `reload_data`: Load raw data từ MySQL → Bronze
- `full_pipeline_job`: Complete ETL (Bronze → Silver → Gold → Platinum)

**Access**: http://localhost:3001

---

### 2️⃣ Demand Forecasting System

**Mô tả**: Hệ thống dự báo nhu cầu sản phẩm sử dụng Machine Learning

**Công nghệ**:
- LightGBM (Gradient Boosting)
- MLflow (Experiment Tracking & Model Registry)
- PySpark (Feature Engineering)
- Delta Lake (Feature Store)

**Pipeline**:

```
1. Feature Engineering (op_build_features)
   gold.fact_order_item + gold.fact_order + gold.dim_customer
   → silver.forecast_features
   Features: lagged values, rolling averages, calendar features

2. Model Training (op_train_model)
   silver.forecast_features
   → LightGBM model
   → MLflow tracking (metrics: RMSE, MAE, sMAPE, R2)

3. Batch Prediction (op_batch_predict)
   Recursive roll-forward prediction for 28 horizons
   → platinum.demand_forecast

4. Monitoring (op_monitor_forecast)
   Compare actuals vs forecasts
   → platinum.forecast_monitoring
```

**Features**:
- ✅ Recursive forecasting (28 ngày)
- ✅ Multi-product, multi-region
- ✅ Confidence intervals (yhat_lo, yhat_hi)
- ✅ Model versioning với MLflow
- ✅ Automated retraining
- ✅ Performance monitoring

**Metrics**:
- RMSE: Root Mean Square Error
- MAE: Mean Absolute Error
- sMAPE: Symmetric Mean Absolute Percentage Error
- R2: Coefficient of Determination

**Dagster Job**: `forecast_job`  
**Schedule**: Daily at 3:00 AM (Asia/Ho_Chi_Minh)

---

### 3️⃣ OLAP Query Window (Pivot Query Interface)

**Mô tả**: Giao diện truy vấn đa chiều (OLAP-style) không cần viết SQL

**Công nghệ**:
- Streamlit (UI Framework)
- Trino (Distributed SQL Engine)
- Delta Lake (Data Source)

**Features**:

1. **Multi-dimensional Analysis**:
   - Time grain: day/week/month/quarter/year
   - Dynamic dimensions (product, customer, region, etc.)
   - Dynamic measures (revenue, orders, units, etc.)

2. **Advanced Aggregations**:
   - ROLLUP: Hierarchical totals
   - GROUPING SETS: Custom aggregation levels
   - NULLS LAST: Subtotals at the end

3. **Security**:
   - Filter validation (chặn SQL injection)
   - Read-only queries
   - Banned keywords: DROP, DELETE, INSERT, UPDATE, etc.

4. **Export**:
   - CSV download (numeric format preserved)
   - Excel download (proper data types)
   - Formatted timestamps

5. **Performance**:
   - Query caching (10 minutes TTL)
   - Result limiting (configurable)
   - Efficient SQL generation

**Supported Tables**:
- `gold.fact_order` (1 row per order)
- `gold.fact_order_item` (1 row per order item)
- `platinum.dm_sales_monthly_category` (monthly aggregates)

**Access**: http://localhost:8501/📊_Query_Window

---

### 4️⃣ AI-Powered Chat Interface

**Mô tả**: Hỏi đáp bằng ngôn ngữ tự nhiên, tự động sinh SQL và RAG-based explanations

**Công nghệ**:
- Streamlit (Frontend)
- FastAPI (Backend - chat_service)
- Qdrant (Vector Database for RAG)
- Gemini/OpenAI (LLM)
- Trino (SQL Execution)

**Features**:

1. **Natural Language to SQL**:
   - Intent recognition (Vietnamese & English)
   - Template-based SQL generation
   - Safe query execution

2. **RAG (Retrieval-Augmented Generation)**:
   - Document embeddings trong Qdrant
   - KPI definitions retrieval
   - Context-aware responses

3. **Session Management**:
   - Chat history
   - Context preservation
   - Query suggestions

**Example Queries**:
- "Doanh thu tháng 3 năm 2018?"
- "Top 10 sản phẩm bán chạy nhất?"
- "Phương thức thanh toán nào phổ biến?"

**Access**: http://localhost:8501/💬_Chat

---

### 5️⃣ BI Dashboard với Metabase

**Mô tả**: Business Intelligence dashboard với native Trino connector

**Features**:
- Visual query builder
- Pre-built dashboards
- Custom SQL queries
- Scheduled reports
- Email alerts

**Sample Dashboards**:
- Monthly Revenue Trend
- Top Categories by Revenue
- Payment Mix Analysis
- Logistics SLA Performance
- Customer Lifecycle Analysis

**Access**: http://localhost:3000

---

## 🛠️ Technology Stack

### Core Technologies

| Category | Technology | Version | Purpose |
|----------|-----------|---------|---------|
| **Storage** | MinIO | Latest | S3-compatible object storage |
| **Metadata** | Hive Metastore | 3.1.2 | Table catalog & schema management |
| **Compute** | Apache Spark | 3.3.2 | Distributed data processing |
| **Lakehouse** | Delta Lake | 2.3.0 | ACID transactions, versioning |
| **Query Engine** | Trino | 414 | Distributed SQL queries |
| **Orchestration** | Dagster | 1.5.x | Workflow management |
| **ML Platform** | MLflow | 2.x | Experiment tracking, model registry |
| **Web Framework** | Streamlit | 1.x | Interactive dashboards |
| **BI Tool** | Metabase | Latest | Business intelligence |
| **Vector DB** | Qdrant | Latest | RAG embeddings |
| **Database** | MySQL | 8.0 | Source data & metadata |

### Programming Languages

- **Python 3.9+**: Main language (Spark, Dagster, ML, Streamlit)
- **SQL**: Trino, Spark SQL, MySQL
- **Bash**: Automation scripts

### Python Libraries

**Data Processing**:
- PySpark 3.3.2
- Pandas 1.5.3
- Polars 0.20.25
- PyArrow 12.0.1

**Machine Learning**:
- LightGBM
- Prophet
- scikit-learn
- MLflow

**Web & API**:
- Streamlit
- FastAPI
- Requests

**Others**:
- Delta-Spark
- Trino connector
- OpenPyXL (Excel export)

---

## 📊 Dataset

### Brazilian E-commerce Dataset (Olist)

**Source**: [Kaggle - Brazilian E-commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

**Statistics**:
- ~100,000 orders
- ~32,000 products
- ~9,000 sellers
- ~99,000 customers
- Period: 2016-2018

**Tables**:
1. `customers` - Customer information
2. `orders` - Order details
3. `order_items` - Order line items
4. `products` - Product catalog
5. `sellers` - Seller information
6. `geolocation` - Geographic data
7. `order_payments` - Payment information
8. `order_reviews` - Customer reviews
9. `product_category_name_translation` - Category translations

---

## 📈 Data Layers & Tables

### Bronze Layer (9 tables)
Raw data extracted from MySQL, partitioned by ingestion date

### Silver Layer (11 tables)
Cleaned, normalized data with quality checks
- 10 base tables
- `forecast_features` (ML features)

### Gold Layer (10 tables)
Star schema with facts and dimensions

**Fact Tables**:
- `fact_order` (96,462 rows)
- `fact_order_item` (112,560 rows)

**Dimension Tables**:
- `dim_customer` (99,439 rows)
- `dim_product` (32,951 rows)
- `dim_seller` (3,095 rows)
- `dim_product_category` (71 rows)
- `dim_date` (731 rows)
- `dim_geolocation` (1,000,000+ rows)
- `dim_payment_type` (distinct payment methods)
- `dim_order_status` (distinct statuses)

### Platinum Layer (8 tables)
Business-ready datamarts

**Datamarts**:
1. `dm_sales_monthly_category` (1,326 rows) - Monthly sales by category
2. `dm_seller_kpi` (3,095 rows) - Seller KPIs
3. `dm_customer_lifecycle` (96,462 rows) - Customer behavior
4. `dm_payment_mix` (90 rows) - Payment analysis
5. `dm_logistics_sla` (574 rows) - Delivery SLA
6. `dm_product_bestsellers` (32,951 rows) - Top products
7. `dm_category_price_bands` (326 rows) - Price segments
8. `demand_forecast` - ML forecasts (28 days × products × regions)

**Total Records**: ~134,824 rows

---

## 🚀 Deployment

### Prerequisites

- Docker & Docker Compose
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space
- Internet connection (for initial setup)

### Quick Start

```bash
# 1. Clone repository
git clone <repository-url>
cd Data_Warehouse_Fresh

# 2. Download JAR dependencies
chmod +x download_jars.sh
./download_jars.sh

# 3. Setup environment
cp env.example .env
# Edit .env if needed

# 4. Start all services
docker-compose up -d

# 5. Wait for services to be healthy (~2 minutes)
docker-compose ps

# 6. Load initial data
make reload-data

# 7. Run ETL pipeline
make run-pipeline

# 8. (Optional) Initialize forecast system
make forecast-init
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Streamlit** | http://localhost:8501 | - |
| **Metabase** | http://localhost:3000 | Setup on first access |
| **Dagster** | http://localhost:3001 | - |
| **Spark Master** | http://localhost:8080 | - |
| **Trino** | http://localhost:8082 | - |
| **MinIO Console** | http://localhost:9001 | minio/minio123 |
| **Jupyter** | http://localhost:8888 | - |
| **MLflow** | http://localhost:5000 | - |

---

## 📝 Recent Improvements & Optimizations

### Streamlit Application (October 2024)

✅ **Export Format Fix**:
- Separated display DataFrame from export DataFrame
- Preserved numeric types for CSV/Excel exports
- Impact: Proper number formatting in Excel

✅ **Date Handling**:
- Added `CAST(date_col AS date)` in WHERE clauses
- Impact: Prevented timezone-related query errors

✅ **Security Enhancement**:
- Filter validation with banned keywords (DROP, DELETE, INSERT, etc.)
- ValueError on dangerous input
- Impact: Basic SQL injection prevention

✅ **OLAP Display Improvement**:
- Added `NULLS LAST` to ORDER BY
- Impact: Subtotals/grand totals appear at the end (better UX)

✅ **No Warnings**:
- Removed duplicate `set_page_config` calls
- Impact: Clean console output

✅ **Docker Health Monitoring**:
- Fixed healthcheck with `|| exit 1`
- Impact: Proper container health detection

✅ **Performance Optimization**:
- Reduced dependencies from 14 to 9 packages
- Removed unused: matplotlib, seaborn, plotly, boto3, pymysql
- Impact: 40% smaller image, faster builds (~30% faster)

✅ **UI Clarity**:
- Port labeled "8082 (ext)" vs "8080 (int)"
- Impact: Clear internal vs external port distinction

### Forecast System (October 2024)

✅ **Schema Alignment**:
- Fixed feature engineering to read from correct gold tables
- Proper joins: fact_order_item + fact_order + dim_customer
- Impact: Accurate revenue calculation

✅ **Recursive Forecasting**:
- Implemented roll-forward prediction logic
- Features updated using previous horizon's yhat
- Impact: More realistic time-series forecasts (not flat)

✅ **Forecast Monitoring**:
- Fully implemented `op_monitor_forecast`
- Calculates abs_error, pct_error, sMAPE
- Writes to `platinum.forecast_monitoring`
- Impact: Automated accuracy tracking

✅ **Spark Configuration**:
- Switched from `spark.jars.packages` to `spark.jars`
- Load pre-downloaded JARs from `/opt/jars`
- Impact: Reliable JAR loading, no Maven dependency

✅ **Bucket Path Correction**:
- Changed from `s3a://warehouse` to `s3a://lakehouse`
- Impact: Correct data access

---

## 🔍 Use Cases & Examples

### Use Case 1: Business Analytics

**Scenario**: Analyze monthly revenue trends by category

**Method**: Query Window

**Steps**:
1. Open http://localhost:8501/📊_Query_Window
2. Select `platinum.dm_sales_monthly_category`
3. Time grain: `month`
4. Dimensions: `product_category_name_english`
5. Measures: `SUM(total_revenue) AS revenue`
6. Date range: Last 6 months
7. Click "Chạy truy vấn"
8. Export to Excel

**Output**: Monthly revenue breakdown by category with proper numeric formatting

---

### Use Case 2: Demand Forecasting

**Scenario**: Forecast product demand for next 28 days

**Method**: Dagster forecast_job

**Steps**:
1. Open http://localhost:3001
2. Jobs → `forecast_job`
3. Launch Run
4. Monitor progress (4 ops: features → train → predict → monitor)
5. Check results in MinIO: `s3a://lakehouse/platinum/demand_forecast`

**Output**: 28-day forecasts with confidence intervals for all products × regions

---

### Use Case 3: Natural Language Queries

**Scenario**: Ask "Top 5 products by revenue in Q3 2018?"

**Method**: Chat Interface

**Steps**:
1. Open http://localhost:8501/💬_Chat
2. Type: "Top 5 products by revenue in Q3 2018?"
3. System generates SQL and executes
4. View results + SQL query

**Output**: Ranked list of top 5 products with revenue amounts

---

## 📚 Documentation Files

Comprehensive documentation available in project root:

1. **FORECAST_FILES.txt** (52KB, 1,391 lines)
   - ML module files
   - Forecast ops & jobs
   - Configuration & examples

2. **QUERY_WINDOW_FILES.txt** (30KB, 819 lines)
   - Query Window implementation (legacy, now merged into Streamlit)
   - OLAP features
   - SQL generation logic

3. **STREAMLIT_APP_FILES.txt** (41KB, 1,168 lines)
   - Complete Streamlit application
   - 3 pages: Main, Query Window, Chat
   - Docker config & dependencies

**Total**: 123KB documentation, 3,378 lines, 18 files documented

---

## 🎯 Performance Metrics

### ETL Pipeline

- Bronze layer loading: ~2 minutes (9 tables)
- Silver layer transformation: ~3 minutes (10 tables)
- Gold layer aggregation: ~2 minutes (10 tables)
- Platinum layer datamarts: ~1 minute (7 tables)
- **Total pipeline time**: ~8-10 minutes

### Forecast System

- Feature engineering: ~2 minutes (~96K rows)
- Model training: ~3-5 minutes (LightGBM with CV)
- Batch prediction: ~1-2 minutes (28 horizons × products × regions)
- **Total forecast time**: ~6-9 minutes

### Query Performance

- Simple fact query: <1 second
- Complex join with aggregations: 2-5 seconds
- ROLLUP query (3 dimensions): 3-7 seconds
- Full table scan (100K rows): 5-10 seconds

### Storage

- Bronze layer: ~150MB (raw data)
- Silver layer: ~200MB (cleaned)
- Gold layer: ~100MB (star schema)
- Platinum layer: ~50MB (aggregates)
- **Total storage**: ~500MB

---

## 🔮 Future Enhancements

### Short-term (Q1 2025)

- [ ] Add chart visualization to Query Window
- [ ] Implement query history & saved templates
- [ ] Add drill-down capability in OLAP
- [ ] Expand Chat intents (20+ templates)
- [ ] Add data quality dashboard
- [ ] Implement incremental forecast updates

### Medium-term (Q2-Q3 2025)

- [ ] User authentication & RBAC
- [ ] Real-time streaming with Kafka
- [ ] Advanced ML models (ARIMA, Prophet ensemble)
- [ ] Multi-model forecasting
- [ ] A/B testing framework
- [ ] Cost optimization dashboard

### Long-term (Q4 2025+)

- [ ] Multi-tenancy support
- [ ] Auto-scaling Spark cluster
- [ ] GPU acceleration for ML
- [ ] Advanced anomaly detection
- [ ] Predictive alerts & recommendations
- [ ] Mobile application

---

## 🤝 Contributing

This is an educational/portfolio project. Feedback and suggestions are welcome!

---

## 📄 License

MIT License - See LICENSE file for details

---

## 👨‍💻 Author

**Truong An**

Data Engineering & Machine Learning Project

Built with ❤️ using Modern Data Stack

---

## 🙏 Acknowledgments

- Brazilian E-commerce dataset: Olist
- Open-source communities: Spark, Trino, Delta Lake, Dagster, MLflow
- Modern Data Stack ecosystem

---

**Last Updated**: October 28, 2024

**Project Status**: ✅ Production-Ready

**Documentation**: ✅ Complete

**Test Coverage**: ✅ Manual testing complete

**Containerization**: ✅ Fully Dockerized

