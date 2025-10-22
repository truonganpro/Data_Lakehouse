# 📊 Hướng Dẫn Cấu Hình và Trực Quan Hóa Dữ Liệu Metabase

## ✅ Cấu Hình Kết Nối Trino trong Metabase

### Bước 1: Truy cập Metabase
- Mở trình duyệt và truy cập: `http://localhost:3000`
- Đăng nhập với tài khoản admin (hoặc tạo tài khoản nếu lần đầu)

### Bước 2: Thêm Database Connection
1. Click vào **Settings** (biểu tượng ⚙️ ở góc trên bên phải)
2. Chọn **Admin settings**
3. Chọn **Databases** trong menu bên trái
4. Click **Add database**

### Bước 3: Cấu Hình Trino Connection
Điền các thông tin sau:

| Field | Value | Ghi chú |
|-------|-------|---------|
| **Database type** | `Trino` | Chọn từ dropdown |
| **Name** | `Platinum Data Warehouse` | Tên tùy ý |
| **Host** | `trino` | Tên container trong Docker network |
| **Port** | `8080` | Port mặc định của Trino |
| **Database name** | *(để trống)* | Không cần thiết |
| **Username** | `metabase` | Bất kỳ username nào |
| **Password** | *(để trống)* | Không cần password |
| **Use a secure connection (SSL)** | ❌ **OFF** | MinIO nội bộ không dùng SSL |
| **Additional JDBC connection string** | `catalog=minio&schema=platinum` | **QUAN TRỌNG** |

> **💡 Lưu ý quan trọng:**
> - **Host phải là `trino`** (không phải `localhost`) vì Metabase chạy trong container
> - **Additional JDBC string** phải có `catalog=minio&schema=platinum` để trỏ đúng vào catalog minio và schema platinum

### Bước 4: Lưu và Sync
1. Click **Save**
2. Sau khi lưu thành công, click **Sync database schema now**
3. Click **Re-scan field values now**

Bạn sẽ thấy 7 tables trong schema platinum:
- `dm_sales_monthly_category` - Doanh số theo tháng và danh mục
- `dm_seller_kpi` - KPI người bán
- `dm_customer_lifecycle` - Vòng đời khách hàng
- `dm_payment_mix` - Phân bổ phương thức thanh toán
- `dm_logistics_sla` - SLA logistics
- `dm_product_bestsellers` - Sản phẩm bán chạy
- `dm_category_price_bands` - Phân khúc giá theo danh mục

---

## 📈 Tạo Dashboard Trực Quan Hóa

### Dashboard 1: Sales Performance Overview

#### Chart 1.1: Doanh Thu Theo Tháng (Line Chart)
```sql
SELECT 
    year_month,
    SUM(gmv) as total_revenue
FROM minio.platinum.dm_sales_monthly_category
GROUP BY year_month
ORDER BY year_month
```
- **Visualization**: Line chart
- **X-axis**: `year_month`
- **Y-axis**: `total_revenue`

#### Chart 1.2: Top 10 Danh Mục Bán Chạy (Bar Chart)
```sql
SELECT 
    product_category_name_english as category,
    SUM(gmv) as total_revenue,
    SUM(orders) as total_orders
FROM minio.platinum.dm_sales_monthly_category
WHERE product_category_name_english IS NOT NULL
GROUP BY product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10
```
- **Visualization**: Bar chart
- **X-axis**: `category`
- **Y-axis**: `total_revenue`

#### Chart 1.3: Orders vs GMV Trend (Multi-line)
```sql
SELECT 
    year_month,
    SUM(orders) as total_orders,
    SUM(gmv) as total_gmv
FROM minio.platinum.dm_sales_monthly_category
GROUP BY year_month
ORDER BY year_month
```
- **Visualization**: Line chart with 2 lines
- **X-axis**: `year_month`
- **Y-axes**: `total_orders`, `total_gmv`

---

### Dashboard 2: Seller Performance

#### Chart 2.1: Top 10 Sellers by Revenue
```sql
SELECT 
    seller_id,
    gmv,
    total_orders,
    total_items,
    avg_order_value
FROM minio.platinum.dm_seller_kpi
ORDER BY gmv DESC
LIMIT 10
```
- **Visualization**: Table

#### Chart 2.2: Seller Distribution by State
```sql
SELECT 
    seller_state,
    COUNT(DISTINCT seller_id) as seller_count,
    SUM(gmv) as total_revenue
FROM minio.platinum.dm_seller_kpi
WHERE seller_state IS NOT NULL
GROUP BY seller_state
ORDER BY total_revenue DESC
```
- **Visualization**: Map (nếu có) hoặc Bar chart

---

### Dashboard 3: Customer Insights

#### Chart 3.1: Customer Lifecycle Funnel
```sql
SELECT 
    order_sequence,
    COUNT(*) as customer_count
FROM minio.platinum.dm_customer_lifecycle
WHERE order_sequence <= 5
GROUP BY order_sequence
ORDER BY order_sequence
```
- **Visualization**: Funnel chart

#### Chart 3.2: Average Order Value by Customer Segment
```sql
SELECT 
    CASE 
        WHEN total_orders = 1 THEN 'One-time'
        WHEN total_orders <= 3 THEN 'Occasional'
        ELSE 'Loyal'
    END as customer_segment,
    AVG(order_total) as avg_order_value,
    COUNT(DISTINCT customer_id) as customer_count
FROM minio.platinum.dm_customer_lifecycle
GROUP BY 
    CASE 
        WHEN total_orders = 1 THEN 'One-time'
        WHEN total_orders <= 3 THEN 'Occasional'
        ELSE 'Loyal'
    END
```
- **Visualization**: Bar chart

---

### Dashboard 4: Payment & Logistics

#### Chart 4.1: Payment Method Mix
```sql
SELECT 
    payment_type,
    SUM(total_value) as total_payment_value,
    SUM(order_count) as total_orders,
    AVG(avg_payment_value) as avg_value
FROM minio.platinum.dm_payment_mix
WHERE payment_type IS NOT NULL
GROUP BY payment_type
ORDER BY total_payment_value DESC
```
- **Visualization**: Pie chart hoặc Donut chart

#### Chart 4.2: Logistics Performance by State
```sql
SELECT 
    seller_state,
    COUNT(*) as total_shipments,
    AVG(delivery_days) as avg_delivery_days,
    AVG(estimated_delivery_days) as avg_estimated_days,
    SUM(CASE WHEN on_time = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as on_time_percentage
FROM minio.platinum.dm_logistics_sla
WHERE seller_state IS NOT NULL
GROUP BY seller_state
ORDER BY total_shipments DESC
LIMIT 15
```
- **Visualization**: Table with conditional formatting

---

### Dashboard 5: Product Analytics

#### Chart 5.1: Top Bestsellers
```sql
SELECT 
    product_id,
    product_category_name_english as category,
    total_quantity_sold,
    total_gmv,
    unique_orders,
    avg_price
FROM minio.platinum.dm_product_bestsellers
WHERE product_category_name_english IS NOT NULL
ORDER BY total_quantity_sold DESC
LIMIT 20
```
- **Visualization**: Table

#### Chart 5.2: Price Band Distribution by Category
```sql
SELECT 
    product_category_name_english as category,
    price_band,
    COUNT(*) as product_count,
    SUM(total_quantity) as total_sold
FROM minio.platinum.dm_category_price_bands
WHERE product_category_name_english IS NOT NULL
GROUP BY product_category_name_english, price_band
ORDER BY category, price_band
```
- **Visualization**: Stacked bar chart

---

## 🔍 Kiểm Tra Nhanh

Chạy các query sau để kiểm tra dữ liệu:

```sql
-- Tổng số records trong mỗi table
SELECT 'dm_sales_monthly_category' as table_name, COUNT(*) as row_count 
FROM minio.platinum.dm_sales_monthly_category
UNION ALL
SELECT 'dm_seller_kpi', COUNT(*) FROM minio.platinum.dm_seller_kpi
UNION ALL
SELECT 'dm_customer_lifecycle', COUNT(*) FROM minio.platinum.dm_customer_lifecycle
UNION ALL
SELECT 'dm_payment_mix', COUNT(*) FROM minio.platinum.dm_payment_mix
UNION ALL
SELECT 'dm_logistics_sla', COUNT(*) FROM minio.platinum.dm_logistics_sla
UNION ALL
SELECT 'dm_product_bestsellers', COUNT(*) FROM minio.platinum.dm_product_bestsellers
UNION ALL
SELECT 'dm_category_price_bands', COUNT(*) FROM minio.platinum.dm_category_price_bands
ORDER BY table_name;
```

**Kết quả mong đợi:**
- `dm_sales_monthly_category`: 1,326 records
- `dm_seller_kpi`: 3,095 records
- `dm_customer_lifecycle`: 96,462 records
- `dm_payment_mix`: 90 records
- `dm_logistics_sla`: 574 records
- `dm_product_bestsellers`: 32,951 records
- `dm_category_price_bands`: 326 records

---

## 🎨 Tips Visualization

1. **Color Coding**: Dùng màu xanh cho positive metrics (revenue, growth), màu đỏ cho negative/warning
2. **Filters**: Thêm date range filter cho dashboard để xem theo khoảng thời gian
3. **Drill-down**: Cho phép click vào chart để xem chi tiết
4. **Refresh Schedule**: Thiết lập auto-refresh dashboard mỗi ngày sau khi ETL chạy

---

## 🚀 Kiến Trúc Hoàn Chỉnh

```
MySQL (Source)
    ↓
Bronze Layer (Raw Data) - MinIO
    ↓
Silver Layer (Cleaned) - MinIO  
    ↓
Gold Layer (Star Schema) - MinIO
    ↓
Platinum Layer (Datamarts) - MinIO ← **Trino queries này**
    ↓
Metabase (Visualization) ← **Bạn đang ở đây**
```

---

## ❓ Troubleshooting

### Metabase không kết nối được Trino
- Kiểm tra container đang chạy: `docker ps | grep trino`
- Test Trino CLI: `docker exec trino trino --execute "SHOW CATALOGS;"`
- Xem logs: `docker logs trino`

### Không thấy tables
- Sync lại database trong Metabase: **Admin → Databases → Sync**
- Kiểm tra Trino có thấy tables: `docker exec trino trino --execute "SHOW TABLES FROM minio.platinum;"`

### Query chậm
- Kiểm tra Spark resources trong `docker-compose.yaml`
- Tăng memory cho Trino nếu cần
- Xem xét partition data theo thời gian

---

**Chúc bạn thành công với dự án Data Warehouse! 🎉**

