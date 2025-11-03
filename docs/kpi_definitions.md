# KPI Definitions - Brazilian E-commerce

## Revenue KPIs

### Total Revenue (Tổng doanh thu)
**Định nghĩa:** Tổng giá trị đơn hàng (bao gồm cả phí ship)

**Formula:**
```sql
SUM(total_price) 
WHERE order_status IN ('delivered', 'shipped', 'invoiced')
```

**Dimensions:** Tháng, Danh mục, Vùng miền

---

### Net Revenue (Doanh thu thuần)
**Định nghĩa:** Doanh thu sau khi trừ đơn hủy và hoàn trả

**Formula:**
```sql
SUM(CASE 
    WHEN order_status = 'delivered' THEN total_price 
    ELSE 0 
END)
```

---

### Average Order Value (AOV - Giá trị đơn hàng trung bình)
**Định nghĩa:** Giá trị trung bình của mỗi đơn hàng

**Formula:**
```sql
AVG(total_price) 
WHERE order_status = 'delivered'
```

**Target:** > 150 BRL

---

## Customer KPIs

### Active Customers (Khách hàng hoạt động)
**Định nghĩa:** Số khách hàng có đơn hàng trong kỳ

**Formula:**
```sql
COUNT(DISTINCT customer_id)
WHERE order_purchase_timestamp >= [start_date]
  AND order_purchase_timestamp < [end_date]
```

---

### Customer Retention Rate (Tỷ lệ giữ chân khách)
**Định nghĩa:** % khách hàng mua lại trong kỳ tiếp theo

**Formula:**
```sql
COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) * 100.0 / 
COUNT(DISTINCT customer_id)
```

**Target:** > 15%

---

## Product KPIs

### Top Products (Sản phẩm bán chạy)
**Định nghĩa:** Sản phẩm có doanh thu cao nhất

**Query:**
```sql
SELECT 
    product_id,
    product_category_name_english,
    SUM(total_price) AS revenue,
    SUM(order_item_quantity) AS qty
FROM gold.fact_order
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 20
```

---

### Category Performance (Hiệu suất danh mục)
**Dimensions:**
- Revenue by category
- Order count by category
- AOV by category

---

## Operational KPIs

### Delivery Performance (Hiệu suất giao hàng)
**Định nghĩa:** % đơn hàng giao đúng hạn

**Formula:**
```sql
SUM(delivered_on_time) * 100.0 / COUNT(*) AS on_time_pct
WHERE order_status = 'delivered'
```

**Target:** > 90%

---

### Average Delivery Time (Thời gian giao hàng trung bình)
**Định nghĩa:** Số ngày trung bình từ đặt hàng đến nhận hàng

**Formula:**
```sql
AVG(
    date_diff('day', 
        order_purchase_timestamp, 
        order_delivered_customer_date)
) AS avg_delivery_days
WHERE order_status = 'delivered'
```

**Target:** < 15 days

---

### Order Status Breakdown (Phân bố trạng thái đơn)
**Categories:**
- `delivered` (đã giao): > 95%
- `canceled` (đã hủy): < 2%
- `shipped` (đang giao): 2-3%
- `invoiced` (đã xuất hóa đơn): < 1%
- `processing` (đang xử lý): < 1%

---

## Payment KPIs

### Payment Method Distribution (Phân bố phương thức thanh toán)
**Methods:**
- `credit_card`: ~75%
- `boleto`: ~20% (thanh toán bằng phiếu ở Brazil)
- `voucher`: ~3%
- `debit_card`: ~2%

---

### Installment Analysis (Phân tích trả góp)
**Average installments:** 2-3 kỳ

**Query:**
```sql
SELECT 
    payment_type,
    AVG(payment_installments) AS avg_installments,
    COUNT(DISTINCT order_id) AS orders
FROM gold.fact_order
WHERE payment_type = 'credit_card'
GROUP BY 1
```

---

## Regional KPIs

### Top States by Revenue (Bang có doanh thu cao nhất)
**Top 3:**
1. SP (São Paulo): ~40%
2. RJ (Rio de Janeiro): ~15%
3. MG (Minas Gerais): ~12%

---

### Regional Distribution (Phân bố theo vùng)
**Regions:**
- Southeast (SP, RJ, MG, ES): ~70%
- South (PR, SC, RS): ~15%
- Northeast: ~10%
- Other: ~5%

---

## Growth KPIs

### Month-over-Month Growth (Tăng trưởng so với tháng trước)
**Formula:**
```sql
WITH monthly AS (
    SELECT 
        date_trunc('month', order_purchase_timestamp) AS month,
        SUM(total_price) AS revenue
    FROM gold.fact_order
    GROUP BY 1
)
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS prev_month,
    (revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 / 
        LAG(revenue) OVER (ORDER BY month) AS growth_pct
FROM monthly
ORDER BY month DESC
```

---

## Forecast KPIs (nếu có ML pipeline)

### Forecast Accuracy (Độ chính xác dự báo)
**Metric:** sMAPE (Symmetric Mean Absolute Percentage Error)

**Target:** < 25%

---

### Demand Forecast (Dự báo nhu cầu)
**Horizons:**
- 7 days: Inventory planning
- 14 days: Procurement planning
- 28 days: Strategic planning

---

## How to Use This Document

1. **Chatbot queries:** Đặt câu hỏi theo định nghĩa KPI
   - "Doanh thu tháng này?"
   - "AOV theo danh mục?"
   - "Top 10 sản phẩm?"

2. **Dashboard creation:** Sử dụng definitions này để tạo charts trong Metabase

3. **SQL templates:** Copy formulas và điều chỉnh theo nhu cầu

---

**Last updated:** 2025-10-25

