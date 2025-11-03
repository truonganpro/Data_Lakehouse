# Data Dictionary - Brazilian E-commerce Lakehouse

## Tổng quan

Data Warehouse này chứa dữ liệu thương mại điện tử từ Olist (Brazil), tổ chức theo kiến trúc Medallion (Bronze → Silver → Gold → Platinum).

## Gold Layer - Production Tables

### 1. `gold.fact_order`

Bảng fact chính chứa thông tin đơn hàng.

**Columns:**
- `order_id` (STRING): ID đơn hàng (unique)
- `customer_id` (STRING): ID khách hàng
- `customer_state` (STRING): Tỉnh/bang của khách hàng (ví dụ: SP, RJ, MG)
- `order_status` (STRING): Trạng thái đơn hàng (delivered, shipped, canceled, etc.)
- `order_purchase_timestamp` (TIMESTAMP): Thời điểm đặt hàng
- `order_approved_at` (TIMESTAMP): Thời điểm duyệt đơn
- `order_delivered_carrier_date` (TIMESTAMP): Thời điểm giao cho vận chuyển
- `order_delivered_customer_date` (TIMESTAMP): Thời điểm giao đến khách
- `order_estimated_delivery_date` (DATE): Ngày dự kiến giao hàng
- `product_id` (STRING): ID sản phẩm
- `product_category_name_english` (STRING): Danh mục sản phẩm (tiếng Anh)
- `seller_id` (STRING): ID người bán
- `seller_state` (STRING): Tỉnh/bang của người bán
- `shipping_limit_date` (TIMESTAMP): Hạn chót giao hàng
- `price` (DECIMAL): Giá sản phẩm
- `freight_value` (DECIMAL): Phí vận chuyển
- `total_price` (DECIMAL): = price + freight_value
- `order_item_quantity` (INT): Số lượng sản phẩm trong đơn
- `payment_type` (STRING): Phương thức thanh toán (credit_card, boleto, voucher, debit_card)
- `payment_installments` (INT): Số kỳ trả góp
- `delivered_on_time` (INT): 1 = đúng hạn, 0 = trễ hạn

**Grain:** Mỗi dòng = 1 order_item (một sản phẩm trong đơn hàng)

**Queries phổ biến:**
- Doanh thu theo tháng: `GROUP BY date_trunc('month', order_purchase_timestamp)`
- Top sản phẩm: `GROUP BY product_id ORDER BY SUM(total_price) DESC`
- Phân bố theo vùng: `GROUP BY customer_state`

---

### 2. `gold.product_dim`

Dimension table cho sản phẩm.

**Columns:**
- `product_id` (STRING): ID sản phẩm (PK)
- `product_category_name` (STRING): Danh mục (tiếng Bồ Đào Nha)
- `product_category_name_english` (STRING): Danh mục (tiếng Anh)
- `product_name_length` (INT): Độ dài tên sản phẩm
- `product_description_length` (INT): Độ dài mô tả
- `product_photos_qty` (INT): Số lượng ảnh
- `product_weight_g` (INT): Trọng lượng (gram)
- `product_length_cm` (INT): Chiều dài (cm)
- `product_height_cm` (INT): Chiều cao (cm)
- `product_width_cm` (INT): Chiều rộng (cm)

**Usage:** JOIN với `fact_order` để lấy thông tin chi tiết sản phẩm

---

### 3. `gold.customer_dim`

Dimension table cho khách hàng.

**Columns:**
- `customer_id` (STRING): ID khách hàng (PK)
- `customer_unique_id` (STRING): ID duy nhất (một người có thể có nhiều customer_id)
- `customer_zip_code_prefix` (STRING): Mã bưu chính
- `customer_city` (STRING): Thành phố
- `customer_state` (STRING): Tỉnh/bang

---

### 4. `gold.seller_dim`

Dimension table cho người bán.

**Columns:**
- `seller_id` (STRING): ID người bán (PK)
- `seller_zip_code_prefix` (STRING): Mã bưu chính
- `seller_city` (STRING): Thành phố
- `seller_state` (STRING): Tỉnh/bang

---

## Platinum Layer - Analytics Tables

### `platinum.demand_forecast`

Bảng dự báo nhu cầu (nếu đã chạy ML pipeline).

**Columns:**
- `forecast_date` (DATE): Ngày được dự báo
- `product_id` (STRING): ID sản phẩm
- `region_id` (STRING): Vùng miền (customer_state)
- `horizon` (INT): Số ngày phía trước (1..28)
- `yhat` (DOUBLE): Dự báo điểm
- `yhat_lo` (DOUBLE): Cận dưới
- `yhat_hi` (DOUBLE): Cận trên
- `model_name` (STRING): Tên model
- `run_id` (STRING): MLflow run ID

---

## Metrics & KPIs

### Doanh thu (Revenue)
```sql
SELECT SUM(total_price) AS revenue
FROM gold.fact_order
WHERE order_purchase_timestamp >= date_add('month', -1, CURRENT_DATE)
```

### Average Order Value (AOV)
```sql
SELECT AVG(total_price) AS aov
FROM gold.fact_order
WHERE order_status = 'delivered'
```

### Delivery Performance
```sql
SELECT 
    SUM(CASE WHEN delivered_on_time = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS on_time_pct
FROM gold.fact_order
WHERE order_status = 'delivered'
```

### Top Categories
```sql
SELECT 
    product_category_name_english,
    SUM(total_price) AS revenue,
    COUNT(DISTINCT order_id) AS orders
FROM gold.fact_order
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
```

---

## Data Quality Notes

- **Nulls:** `product_category_name_english` có thể NULL cho một số sản phẩm
- **Duplicates:** Mỗi `order_id` có thể có nhiều dòng (nhiều sản phẩm)
- **Date range:** Dữ liệu từ 2016-09 đến 2018-10
- **States:** 27 bang của Brazil (SP, RJ, MG là lớn nhất)

---

## Contact & Support

Nếu có câu hỏi về dữ liệu, liên hệ Data Team.

