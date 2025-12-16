# Hệ thống Dự báo Nhu cầu (Demand Forecasting)

## Tổng quan

Hệ thống dự báo nhu cầu sử dụng mô hình LightGBM để dự báo doanh thu (revenue) và số lượng (quantity) cho từng cặp sản phẩm × vùng theo horizon 1-28 ngày.

## Pipeline Dagster

1. **build_features** → Tạo features từ dữ liệu lịch sử (lag, rolling, calendar, target encoding)
2. **train_lightgbm** → Huấn luyện mô hình trên các series "dense" (đủ dữ liệu)
3. **batch_predict** → Dự báo cho tất cả series (LightGBM cho dense, baseline cho sparse)
4. **backtest** → Đánh giá chất lượng forecast và lưu metrics vào monitoring tables

## Bảng dữ liệu Forecast

### `platinum.demand_forecast_revenue`
Chứa dự báo doanh thu cho các cặp product_id × region_id.

**Các cột chính:**
- `forecast_date`: Ngày được dự báo
- `product_id`, `region_id`: Series identifier
- `horizon`: Số ngày từ ngày chạy model (1-28)
- `yhat`: Dự báo điểm (trung bình)
- `yhat_lo`: Biên dưới khoảng tin cậy (CI - Confidence Interval)
- `yhat_hi`: Biên trên khoảng tin cậy (CI)
- `target_type`: "revenue"
- `model_name`: "lightgbm_revenue_global"
- `run_id`: MLflow run ID

### `platinum.demand_forecast_quantity`
Tương tự như `demand_forecast_revenue` nhưng dự báo số lượng (quantity) thay vì doanh thu.

### `platinum.forecast_monitoring`
Chứa metrics monitoring chất lượng forecast (sMAPE, MAE, CI coverage) theo horizon và model type.

### `platinum.forecast_backtest_detail`
Chi tiết kết quả backtest cho từng series, ngày, horizon.

## Giải thích các thuật ngữ

### Horizon (H1, H7, H28...)
- **Horizon = 1**: Dự báo cho ngày mai
- **Horizon = 7**: Dự báo cho 7 ngày nữa
- **Horizon = 28**: Dự báo cho 28 ngày nữa

Horizon càng lớn, độ chính xác thường giảm dần (horizon decay).

### Confidence Interval (CI) - Khoảng tin cậy
- **yhat_lo** (Low): Kịch bản thận trọng - dự báo thấp nhất có thể
- **yhat** (Base): Kịch bản cơ sở - dự báo trung bình
- **yhat_hi** (High): Kịch bản lạc quan - dự báo cao nhất có thể

**CI Width = yhat_hi - yhat_lo**: Độ rộng khoảng tin cậy. CI càng rộng → độ không chắc chắn càng cao.

**CI Coverage**: Tỷ lệ giá trị thực tế nằm trong khoảng [yhat_lo, yhat_hi]. Coverage lý tưởng: 80-90%.

### sMAPE (Symmetric Mean Absolute Percentage Error)
Công thức: `sMAPE = 100 * mean(|y_actual - yhat| / ((|y_actual| + |yhat|) / 2))`

- **sMAPE < 20%**: Rất tốt
- **sMAPE 20-30%**: Tốt
- **sMAPE 30-40%**: Trung bình
- **sMAPE > 40%**: Cần cải thiện

sMAPE là metric chuẩn cho forecast vì đối xứng và không bị ảnh hưởng bởi scale.

### Kịch bản Planning

Khi lập kế hoạch, có 3 kịch bản:

1. **Kịch bản Thận trọng (Conservative/Low)**: Dùng `yhat_lo`
   - Dự trữ tồn kho nhiều hơn
   - Đảm bảo không thiếu hàng
   - Phù hợp khi rủi ro thiếu hàng cao

2. **Kịch bản Cơ sở (Base)**: Dùng `yhat`
   - Kế hoạch chuẩn
   - Dựa trên dự báo trung bình

3. **Kịch bản Lạc quan (Optimistic/High)**: Dùng `yhat_hi`
   - Tăng marketing, mở rộng
   - Phù hợp khi có chiến dịch đặc biệt

### Top/Bottom Movers

- **Top Movers**: Series có forecast cao nhất (nhu cầu tăng mạnh)
- **Bottom Movers**: Series có forecast thấp nhất (nhu cầu giảm)

Giúp nhận diện cơ hội và rủi ro nhanh.

## Series Dense vs Sparse

Hệ thống phân loại series thành 2 nhóm:

### Dense Series (Đủ dữ liệu)
- `n_days >= 90`: Ít nhất 3 tháng có bán
- `total_revenue >= 1000`: Tổng doanh thu đạt ngưỡng
- Dùng mô hình LightGBM chính để dự báo

### Sparse Series (Ít dữ liệu)
- Không đủ điều kiện dense
- Dùng baseline đơn giản: trung bình 28 ngày gần nhất
- Đảm bảo vẫn có dự báo cho tất cả series

## Câu hỏi mẫu cho Chat Service

1. "Dự báo doanh thu 7 ngày tới cho từng bang?"
2. "Top 10 sản phẩm có forecast cao nhất tuần này?"
3. "sMAPE trung bình của model forecast tháng 9/2018?"
4. "Kịch bản thận trọng và lạc quan khác nhau bao nhiêu phần trăm?"
5. "CI coverage của model forecast là bao nhiêu?"

