# 📈 Forecast Dashboard - Cải tiến và Tính năng mới

## ✅ Đã hoàn thành

### 1. 📝 Tooltips/Help Text

#### Bộ lọc
- **Product ID / Region ID**: Tooltip giải thích chọn chuỗi dự báo theo sản phẩm/khu vực. "Tất cả" = tổng hợp tất cả series.
- **Horizon (ngày)**: Tooltip giải thích số ngày dự báo tính từ ngày gần nhất có dữ liệu thật; H=1 là ngày kế tiếp, H=7 là ngày thứ 7.
- **Khoảng ngày**: Tooltip giải thích filter theo `forecast_date` (ngày được dự báo).
- **Expander "Hướng dẫn sử dụng bộ lọc"**: Chi tiết hơn về cách sử dụng từng filter.

#### KPIs
- **Forecast TB**: Tooltip giải thích giá trị dự báo trung bình (trên tất cả series & horizons đang lọc). Dùng để nhìn "mặt bằng" nhu cầu.
- **Forecast Max / Min**: Tooltip giải thích cực trị trong tập đang lọc → phát hiện series "nóng/lạnh".
- **Records**: Tooltip giải thích số dòng dự báo đã nạp = **số series × số horizons**. Giúp ước lượng coverage (độ phủ) của hệ thống.
- **Expander "Giải thích các chỉ số"**: Chi tiết hơn về từng KPI.

#### Biểu đồ
- **Forecast Trend**: Expander giải thích đường Forecast (yhat), dải CI (yhat_lo/hi), và xu hướng.
- **Forecast theo Horizon**: Expander giải thích giá trị trung bình theo từng horizon và sai số tích lũy.

---

### 2. 🎯 Header phụ và Badge thông tin Model

#### Header phụ
- Hiển thị thông tin ngắn gọn về giá trị dự báo (yhat), khoảng tin cậy (yhat_lo/hi), horizon, và records.
- Đặt ở đầu tab Forecast với style nổi bật.

#### Badge thông tin Model
- **Model**: Tên mô hình (LightGBM)
- **Run ID**: ID của lần chạy model (nếu có)
- **Generated at**: Thời gian tạo forecast (nếu có)
- Tự động phát hiện các cột có sẵn trong bảng và hiển thị tương ứng.

---

### 3. 📊 KPI Strips với Scenario (Low/Base/High)

#### Kịch bản dự báo
- **🔴 Kịch bản thận trọng (Low)**: Tổng yhat_lo - dùng cho kế hoạch "worst case"
- **🟡 Kịch bản cơ sở (Base)**: Tổng yhat - dự báo trung bình
- **🟢 Kịch bản lạc quan (High)**: Tổng yhat_hi - dùng cho kế hoạch "best case"
- Hiển thị delta so với base scenario
- Expander giải thích từng kịch bản

---

### 4. ⚠️ Section Rủi ro (Độ bất định)

#### CI Width
- **CI Width TB**: Khoảng tin cậy trung bình (yhat_hi - yhat_lo)
- **Top 10 series có CI rộng nhất**: Bảng danh sách các series có độ bất định cao
- **Chart CI Width theo ngày**: Biểu đồ đường thể hiện xu hướng CI width theo thời gian
- Expander giải thích độ bất định và cách đọc

---

### 5. 📉 Section Horizon Decay

#### Độ dốc theo Horizon
- Tự động tính toán % decay giữa H1 và H7 (hoặc theo range đã chọn)
- Hiển thị màu cảnh báo:
  - 🔴 Nếu giảm >10%: Cảnh báo mạnh
  - 🟡 Nếu giảm >5%: Cảnh báo nhẹ
  - 🟢 Nếu giảm <5%: Bình thường
- Tooltip giải thích: "Nếu giảm >10%, cân nhắc rút ngắn horizon cho quyết định tồn kho."

---

### 6. 🎯 Section Top/Bottom Movers

#### Ưu tiên vận hành
- **Top 10 (Cao nhất)**: Bảng xếp hạng các series có đóng góp forecast cao nhất
- **Bottom 10 (Thấp nhất)**: Bảng xếp hạng các series có đóng góp forecast thấp nhất
- Hiển thị: product_id, region_id, sum_yhat, avg_ci_width
- Expander giải thích:
  - **Top series** kéo trung bình lên (gần Max) nên ưu tiên tồn kho/marketing.
  - **Bottom series** (gần Min) rà soát tránh overstock.

---

### 7. 📊 Section Pareto 80/20

#### Phân tích Pareto
- **Số series cho 80%**: Số series chiếm 80% tổng forecast
- **Tỷ lệ series**: Phần trăm series trong tổng số
- **Tổng forecast**: Tổng giá trị dự báo
- **Pareto Chart**: Biểu đồ kết hợp bar chart (Forecast Value) và line chart (Cumulative %)
- Expander giải thích: Xem **bao nhiêu series** chiếm **80%** tổng forecast. Giúp tập trung nguồn lực vào nhóm series quan trọng nhất.

---

### 8. 🔧 Section Khai thác nâng cao - SQL Mẫu

#### 6 Tab SQL mẫu:

1. **Tổng cung - Kịch bản**
   - Sum theo kịch bản (Low/Base/High) theo ngày
   - Dùng cho lập kế hoạch "base/low/high"

2. **Top/Bottom Movers**
   - Top 10 product × region đóng góp forecast
   - Sắp xếp theo sum_forecast DESC

3. **Độ bất định (CI)**
   - Series có CI trung bình rộng nhất
   - Sắp xếp theo avg_ci_width DESC

4. **Horizon Decay**
   - Độ dốc (H1 vs H7) theo series
   - Tính % decay và sắp xếp theo decay_pct ASC (âm mạnh = giảm mạnh theo H)

5. **Pareto 80/20**
   - Số series chiếm 80% tổng forecast
   - Sử dụng window functions để tính cumulative sum

6. **Phân bố theo vùng**
   - Tổng forecast theo region_id
   - Dùng để vẽ choropleth/map

Tất cả SQL queries đều:
- Tự động áp dụng filters hiện tại (product_id, region_id, date range)
- Sẵn sàng copy-paste vào Trino/Metabase
- Có comment giải thích

---

## 🎨 Cải thiện UX

### Expander cho Tooltips
- Tất cả các section đều có expander "ℹ️ Giải thích..." để người dùng có thể mở/đóng khi cần
- Mặc định đóng để không làm rối giao diện
- Nội dung giải thích ngắn gọn, dễ hiểu

### Badge và Header
- Header phụ với thông tin tổng quan về forecast
- Badge thông tin model với style nổi bật
- Tự động phát hiện các cột có sẵn (run_id, generated_at)

### Charts cải thiện
- **Forecast Trend**: Hover mode 'x unified' để dễ đọc
- **Horizon Chart**: Bar chart với labels rõ ràng
- **CI Width Chart**: Line chart thể hiện xu hướng
- **Pareto Chart**: Combo chart với 2 y-axis (Forecast Value + Cumulative %)

### Error Handling
- Kiểm tra sự tồn tại của các cột trước khi query
- Xử lý lỗi gracefully khi tính toán
- Hiển thị thông báo rõ ràng khi không có dữ liệu

---

## 📋 Cấu trúc File

File: `app/pages/3_📈_Forecast_Explorer.py`

### Sections chính:
1. **Header phụ** - Thông tin tổng quan
2. **Badge thông tin Model** - Model, Run ID, Generated at
3. **Bộ lọc** - Product ID, Region ID, Horizon, Date range
4. **KPIs Tổng quan** - Forecast TB, Max, Min, Records
5. **KPI Strips Scenario** - Low/Base/High
6. **Biểu đồ Forecast Trend** - Với CI band
7. **Biểu đồ Forecast theo Horizon** - Bar chart
8. **Section Rủi ro** - CI width analysis
9. **Section Top/Bottom Movers** - Ranking tables
10. **Section Pareto 80/20** - Pareto analysis với chart
11. **Section Khai thác nâng cao** - SQL mẫu (6 tabs)
12. **Dữ liệu chi tiết** - Dataframe với download CSV

---

## 🚀 Cách sử dụng

### 1. Xem Forecast
- Chọn filters (Product ID, Region ID, Horizon, Date range)
- Xem KPIs và biểu đồ forecast
- Xem các section phân tích (Rủi ro, Movers, Pareto)

### 2. Khai thác nâng cao
- Mở expander "🔧 Khai thác nâng cao - SQL Mẫu"
- Chọn tab SQL phù hợp
- Copy SQL query và chạy trên Trino/Metabase
- Tùy chỉnh SQL nếu cần (thêm filters, thay đổi limit, v.v.)

### 3. Đọc Tooltips
- Click vào icon ℹ️ hoặc expander để xem giải thích
- Hover vào các metric để xem tooltip ngắn
- Đọc header phụ để hiểu tổng quan

---

## 📝 Lưu ý

### Dữ liệu
- Dự báo là **theo series (product × region) và theo ngày**
- Mỗi dòng gồm `forecast_date`, `horizon`, `yhat`, `yhat_lo`, `yhat_hi`, `run_id`, `generated_at`
- CI hiện xây dựng **±15%** quanh yhat (ước lượng đơn giản)
- Khi filter tất cả sản phẩm/khu vực, các biểu đồ là **tổng hợp**

### Performance
- LIMIT 1000 rows cho query chính
- Các tính toán được thực hiện trên DataFrame (in-memory)
- Cache 10 phút cho các query (TTL=600)

### Tương thích
- Tự động phát hiện các cột có sẵn (run_id, generated_at)
- Xử lý gracefully khi thiếu cột
- Hoạt động với cả bảng có/không có model info

---

## 🎯 Gợi ý đọc nhanh

### Top series
- Kéo trung bình lên (gần Max) nên ưu tiên tồn kho/marketing

### Bottom series
- Gần Min) rà soát tránh overstock

### CI bất thường
- Rất rộng hoặc giãn nhanh theo H) → kiểm tra lại dữ liệu đầu vào/đặc trưng

### Horizon decay
- Nếu giảm >10%, cân nhắc rút ngắn horizon cho quyết định tồn kho

---

## ✅ Checklist

- [x] Tooltips/help text cho bộ lọc
- [x] Tooltips cho KPIs
- [x] Header phụ với thông tin model
- [x] Badge thông tin model (run_id, generated_at)
- [x] KPI strips với scenario (Low/Base/High)
- [x] Section Rủi ro (CI width)
- [x] Section Horizon decay
- [x] Section Top/Bottom movers
- [x] Section Pareto 80/20
- [x] SQL queries mẫu (6 tabs)
- [x] Error handling
- [x] Auto-detect columns
- [x] Charts cải thiện
- [x] Expander cho tooltips

---

## 📚 Tài liệu tham khảo

- **SQL Templates**: Tất cả SQL mẫu có sẵn trong tab "Khai thác nâng cao"
- **Tooltips**: Tất cả giải thích có trong expander "ℹ️ Giải thích..."
- **Header phụ**: Giải thích ngắn gọn về forecast, CI, horizon, records

---

**Last Updated:** $(date)

**File:** `app/pages/3_📈_Forecast_Explorer.py`

**Status:** ✅ Hoàn thành

