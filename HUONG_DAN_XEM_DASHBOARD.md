# 📊 HƯỚNG DẪN XEM EXECUTIVE DASHBOARD

## 🌐 CÁCH TRUY CẬP

### Bước 1: Mở trình duyệt
- Mở Chrome, Firefox, Safari, hoặc bất kỳ trình duyệt nào
- Đảm bảo bạn đang ở máy local (không phải qua VPN/remote)

### Bước 2: Truy cập Streamlit
**URL chính:**
```
http://localhost:8501
```

### Bước 3: Vào Executive Dashboard
**Có 2 cách:**

**Cách 1: Qua Navigation (Khuyến nghị)**
1. Vào http://localhost:8501
2. Trong sidebar bên trái, tìm và click vào **"👔 Executive Dashboard"**
3. Hoặc click vào button **"👔 Executive Dashboard"** ở trang chủ

**Cách 2: Truy cập trực tiếp**
```
http://localhost:8501/👔_Executive_Dashboard
```

---

## ⚙️ KIỂM TRA SERVICES

Trước khi xem Dashboard, đảm bảo các services đang chạy:

```bash
# Kiểm tra trạng thái
docker-compose ps

# Services cần thiết:
# ✅ streamlit - Port 8501 (healthy)
# ✅ trino - Port 8082 (healthy)
# ✅ hive-metastore - Port 9083 (running)
# ✅ minio - Port 9000/9001 (running)
```

### Nếu service không chạy:
```bash
# Khởi động tất cả services
docker-compose up -d

# Hoặc khởi động từng service
docker-compose up -d streamlit trino hive-metastore minio
```

---

## 📊 NẾU KHÔNG THẤY DỮ LIỆU

Dashboard có thể mở nhưng không hiển thị biểu đồ nếu **chưa có dữ liệu** trong các bảng Platinum.

### Kiểm tra dữ liệu:
```bash
# Kiểm tra xem có bảng platinum không
docker-compose exec trino trino --execute "SHOW TABLES FROM lakehouse.platinum;"

# Kiểm tra số dòng trong bảng chính
docker-compose exec trino trino --execute "SELECT COUNT(*) FROM lakehouse.platinum.dm_sales_monthly_category;"
```

### Chạy ETL Pipeline để tạo dữ liệu:

**Cách 1: Chạy ETL job qua Dagster**
```bash
docker-compose exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data
```

**Cách 2: Chạy full setup (nếu chưa setup)**
```bash
./full_setup.sh --fresh
```

**Cách 3: Chạy ETL thủ công**
```bash
# Đợi 5-10 phút sau khi chạy ETL
# Sau đó refresh lại Dashboard trong trình duyệt
```

---

## 🎯 CÁCH SỬ DỤNG DASHBOARD

### Sidebar Filters (Bộ lọc):
- **Khoảng thời gian**: Chọn "2017", "2018", "Last 12M", hoặc "Custom"
- **Basis**: 
  - "Purchase (Sales)" - Dữ liệu theo ngày mua
  - "Delivery (SLA)" - Dữ liệu theo ngày giao hàng
- **Danh mục**: Chọn category (nếu có dữ liệu)
- **Bang/State**: Chọn state (nếu có dữ liệu)
- **Top-N**: Slider để chọn số lượng items (5-50)

### 10 Tabs Dashboard:
1. **Executive** - Tổng quan KPI (GMV, Orders, Units, AOV)
2. **Growth** - Phân tích tăng trưởng (MoM, YoY, Pareto)
3. **Category/Product** - Hiệu suất danh mục và sản phẩm
4. **Geography** - Phân bố địa lý, on-time rate theo state
5. **Seller** - Hiệu suất seller (on-time, cancel, review)
6. **Operations** - Logistics SLA (delivery days, on-time trend)
7. **Customer** - Customer lifecycle và cohort retention
8. **Finance** - Payment mix và installments
9. **Forecast** - Dự báo nhu cầu (nếu có bảng forecast)
10. **Data Quality** - Chất lượng dữ liệu (nội bộ)

---

## 🔧 TROUBLESHOOTING

### 1. Dashboard mở nhưng trống (không có biểu đồ)
**Nguyên nhân**: Chưa có dữ liệu trong các bảng platinum

**Giải pháp**:
```bash
# Chạy ETL pipeline
docker-compose exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data

# Đợi 5-10 phút, sau đó refresh trình duyệt
```

### 2. Lỗi "Cannot connect to Trino"
**Nguyên nhân**: Trino service không chạy hoặc network issue

**Giải pháp**:
```bash
# Kiểm tra Trino
docker-compose ps trino

# Restart Trino
docker-compose restart trino

# Đợi 30 giây rồi thử lại
```

### 3. Lỗi "Table not found"
**Nguyên nhân**: ETL chưa chạy hoặc chưa hoàn thành

**Giải pháp**:
```bash
# Kiểm tra logs ETL
docker-compose logs etl_pipeline --tail 50

# Chạy lại ETL nếu cần
docker-compose exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data
```

### 4. Filters không có options (như trong hình)
**Nguyên nhân**: 
- Chưa có dữ liệu trong bảng `dm_sales_monthly_category`
- Hoặc query lấy categories/states bị lỗi

**Giải pháp**:
```bash
# Kiểm tra xem có dữ liệu không
docker-compose exec trino trino --execute "
  SELECT COUNT(*) FROM lakehouse.platinum.dm_sales_monthly_category;
  SELECT DISTINCT category FROM lakehouse.platinum.dm_sales_monthly_category LIMIT 10;
"
```

---

## ✅ CHECKLIST TRƯỚC KHI XEM

- [ ] Services đang chạy: `docker-compose ps`
- [ ] Streamlit healthy: http://localhost:8501/_stcore/health
- [ ] Trino healthy: http://localhost:8082
- [ ] Đã chạy ETL pipeline để có dữ liệu
- [ ] Đã đợi ETL hoàn thành (5-10 phút)
- [ ] Đã refresh trình duyệt sau khi ETL xong

---

## 📞 LIÊN HỆ / HỖ TRỢ

Nếu vẫn gặp vấn đề:
1. Kiểm tra logs: `docker-compose logs streamlit --tail 50`
2. Kiểm tra logs Trino: `docker-compose logs trino --tail 50`
3. Kiểm tra logs ETL: `docker-compose logs etl_pipeline --tail 50`

---

**🎉 Chúc bạn sử dụng Dashboard thành công!**
