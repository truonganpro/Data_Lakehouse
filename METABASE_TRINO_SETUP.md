# 🔗 Hướng dẫn kết nối Metabase với Trino

## 📋 Yêu cầu tiên quyết
- Đảm bảo tất cả services đã chạy: `docker-compose up -d`
- Kiểm tra Trino đang chạy tại: http://localhost:8082
- Đảm bảo ETL pipeline đã chạy và có dữ liệu trong Platinum layer

## 🚀 Các bước kết nối Metabase với Trino

### Bước 1: Truy cập Metabase
1. Mở browser và truy cập: http://localhost:3000
2. Tạo tài khoản admin (lần đầu tiên)

### Bước 2: Thêm Database Connection
1. Vào **Admin** → **Databases** → **Add database**
2. Chọn **Trino** từ danh sách database types

### Bước 3: Cấu hình kết nối Trino

**Thông tin kết nối:**
```
Display Name: Brazilian E-commerce Data Warehouse
Host: trino
Port: 8080
Database: lakehouse  (hoặc để trống)
Username: (để trống)
Password: (để trống)
```

**Advanced Options (Optional):**
```
Schema Filter Type: All schemas
Additional Options: 
- Extra connection string options: `catalog=lakehouse`
```

### Bước 4: Kiểm tra kết nối
1. Click **Test Connection** để kiểm tra
2. Nếu thành công, click **Save**

## 📊 Các bảng có sẵn cho BI

Sau khi kết nối thành công, bạn sẽ thấy các catalog và schema:

### Catalog: `lakehouse`
- **Schema**: `platinum` (chứa datamarts cho BI)

#### Các Datamarts trong Platinum layer:
1. **`dmsalesmonthlycategory`** - Doanh số theo tháng và danh mục
   - Cột: year_month, product_category_name_english, gmv, orders, units, aov

2. **`dmsellerkpi`** - KPI người bán  
   - Cột: seller_id, gmv, orders, units, avg_review_score, on_time_rate, cancel_rate

3. **`dmcustomerlifecycle`** - Phân tích lifecycle khách hàng
   - Cột: customer_id, customer_unique_id, year_month, cohort_month, orders, gmv

4. **`dmpaymentmix`** - Phân tích các loại thanh toán
   - Cột: year_month, payment_type, orders, unique_customers, payment_total

5. **`dmlogisticssla`** - Phân tích logistics và SLA
   - Cột: year_month, geolocation_state, avg_delivered_days, on_time_rate, late_orders

6. **`dmproductbestsellers`** - Top sản phẩm bán chạy
   - Cột: product_id, product_category_name_english, gmv, units, orders, avg_review_score, rank_in_category

7. **`dmcategorypricebands`** - Phân tích giá theo danh mục
   - Cột: product_category_name_english, price_band, order_items, orders, total_gmv, avg_price

### Catalog: `hive`
- **Schema**: `bronze`, `silver`, `gold`, `platinum` (nếu cần truy cập chi tiết hơn)

## 🎨 Tạo Dashboard trong Metabase

### Dashboard mẫu: Brazilian E-commerce Analytics

1. **Tạo Dashboard mới**: Dashboard → New Dashboard

2. **Thêm các câu hỏi (Questions)**:

#### Doanh số theo tháng
```sql
SELECT 
    year_month,
    SUM(gmv) as total_gmv,
    SUM(orders) as total_orders,
    AVG(aov) as avg_order_value
FROM lakehouse.platinum.dmsalesmonthlycategory 
GROUP BY year_month 
ORDER BY year_month;
```

#### Top 10 danh mục sản phẩm
```sql
SELECT 
    product_category_name_english,
    SUM(gmv) as total_gmv,
    SUM(units) as total_units
FROM lakehouse.platinum.dmsalesmonthlycategory 
WHERE year_month = '2018-01'  -- Thay đổi tháng
GROUP BY product_category_name_english 
ORDER BY total_gmv DESC 
LIMIT 10;
```

#### KPI người bán
```sql
SELECT 
    seller_id,
    gmv,
    orders,
    ROUND(avg_review_score, 2) as review_score,
    ROUND(on_time_rate * 100, 2) as on_time_percentage
FROM lakehouse.platinum.dmsellerkpi 
ORDER BY gmv DESC 
LIMIT 20;
```

## 🔧 Troubleshooting

### Lỗi kết nối
1. **Connection refused**: Kiểm tra Trino có đang chạy không
   ```bash
   docker-compose logs trino
   ```

2. **Schema không tìm thấy**: Đảm bảo ETL pipeline đã chạy
   ```bash
   # Kiểm tra trong Dagster UI: http://localhost:3001
   ```

3. **Table không tồn tại**: Chạy lại full pipeline job trong Dagster

### Kiểm tra dữ liệu trực tiếp với Trino CLI
```bash
# Kết nối vào Trino container
docker exec -it trino trino

# Trong Trino CLI:
SHOW CATALOGS;
USE lakehouse.platinum;
SHOW TABLES;
SELECT * FROM dmsalesmonthlycategory LIMIT 10;
```

## 📈 Dashboard Templates

Bạn có thể import các dashboard templates có sẵn hoặc tạo mới dựa trên các datamarts trong Platinum layer để có dashboard hoàn chỉnh cho business intelligence.
