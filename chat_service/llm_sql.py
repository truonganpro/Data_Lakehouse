# -*- coding: utf-8 -*-
"""
LLM-based SQL generation module using Google Gemini
"""
import os
import re
import google.generativeai as genai


READONLY_PAT = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)

PROMPT_SQL = """Bạn là trợ lý dữ liệu giúp VIẾT SQL cho Trino. Chỉ trả về MỘT câu SQL hợp lệ,

YÊU CẦU:
- Chỉ dùng catalog lakehouse và schema gold|platinum (VD: lakehouse.gold.fact_order).
- Chỉ SELECT/WITH (read-only). Không được DELETE, DROP, INSERT, UPDATE, ALTER, CREATE, TRUNCATE.
- Nếu không có GROUP BY thì PHẢI có LIMIT (tối đa 200).
- Ưu tiên đọc từ platinum.* nếu câu hỏi tổng hợp theo tháng/danh mục; nếu cần chi tiết thì gold.*.
- Cột ngày trong gold.fact_order là full_date (DATE). Tháng dùng date_trunc('month', full_date).
- Nếu câu hỏi không nói rõ thời gian, mặc định 3 tháng gần nhất: WHERE full_date >= date_add('month', -3, CURRENT_DATE)
- Column names: customer_state, seller_state, primary_payment_type, payment_total, delivered_on_time, is_canceled
- Luôn thêm WHERE full_date IS NOT NULL cho các bảng fact có cột full_date

SCHEMA (Rút gọn):

**gold.fact_order** (Fact table chính)
- full_date (DATE): Ngày đặt hàng
- order_id (STRING): ID đơn hàng
- customer_id (STRING): ID khách hàng
- customer_state (STRING): Tỉnh/bang của khách hàng (SP, RJ, MG, ...)
- order_status (STRING): Trạng thái (delivered, shipped, canceled, ...)
- items_count (INT): Số lượng items trong đơn
- sum_price (DECIMAL): Tổng giá sản phẩm
- sum_freight (DECIMAL): Tổng phí vận chuyển
- payment_total (DECIMAL): Tổng thanh toán (= sum_price + sum_freight)
- primary_payment_type (STRING): Phương thức thanh toán chính (credit_card, boleto, voucher, debit_card)
- delivered_on_time (INT): 1 = đúng hạn, 0 = trễ
- is_canceled (INT): 1 = đã hủy, 0 = không

**gold.fact_order_item** (Chi tiết items trong đơn)
- full_date (DATE): Ngày đặt hàng
- order_id (STRING): ID đơn hàng
- order_item_id (STRING): ID item
- product_id (STRING): ID sản phẩm
- seller_id (STRING): ID người bán
- price (DECIMAL): Giá sản phẩm
- freight_value (DECIMAL): Phí vận chuyển

**gold.dim_seller** (Dimension người bán)
- seller_id (STRING): ID người bán (PK)
- city_state (STRING): Thành phố + Bang

**gold.dim_product** (Dimension sản phẩm)
- product_id (STRING): ID sản phẩm (PK)
- product_category_name (STRING): Danh mục (tiếng Bồ Đào Nha)

**gold.dim_product_category** (Dimension danh mục)
- product_category_name (STRING): Tên danh mục (tiếng Bồ Đào Nha) (PK)
- product_category_name_english (STRING): Tên danh mục (tiếng Anh)

**platinum.dm_payment_mix** (Aggregate payment by month)
- year_month (STRING): Tháng (YYYY-MM)
- payment_type (STRING): Phương thức thanh toán
- orders (BIGINT): Số đơn hàng
- unique_customers (BIGINT): Số khách hàng unique
- payment_total (DECIMAL): Tổng thanh toán

**platinum.demand_forecast_revenue** (Dự báo nhu cầu doanh thu theo ngày & horizon)
- forecast_date (TIMESTAMP): Ngày dự báo (ngày mà yhat áp dụng)
- product_id (STRING): ID sản phẩm
- region_id (STRING): Mã vùng/bang (SP, RJ, MG, ...)
- horizon (BIGINT): Số ngày từ ngày chạy model (1 = ngày mai, 7 = 7 ngày nữa)
- yhat (DOUBLE): Giá trị dự báo trung bình (doanh thu)
- yhat_lo (DOUBLE): Biên dưới khoảng tin cậy (thận trọng)
- yhat_hi (DOUBLE): Biên trên khoảng tin cậy (lạc quan)
- target_type (STRING): "revenue" hoặc "quantity"
- model_name (STRING): Tên model (vd: "lightgbm_revenue_global")
- model_version (STRING): Phiên bản model
- run_id (STRING): MLflow run ID
- generated_at (TIMESTAMP): Thời điểm tạo dự báo

**platinum.demand_forecast_quantity** (Dự báo nhu cầu số lượng theo ngày & horizon)
- Các cột tương tự như demand_forecast_revenue nhưng target_type = "quantity"
- yhat, yhat_lo, yhat_hi là số lượng đơn vị (không phải doanh thu)

**platinum.forecast_monitoring** (Monitoring độ chính xác forecast - nếu có)
- date (DATE): Ngày thực tế
- product_id (STRING), region_id (STRING), horizon (INT)
- y_actual (DOUBLE): Giá trị thực tế
- yhat, yhat_lo, yhat_hi (DOUBLE): Giá trị dự báo tương ứng
- abs_error (DOUBLE): |y_actual - yhat|
- pct_error (DOUBLE): % sai số tuyệt đối
- smape (DOUBLE): sMAPE (thước đo chuẩn cho forecast)
- model_name (STRING), run_id (STRING)

**platinum.forecast_backtest_detail** (Chi tiết backtest - nếu có)
- cutoff_date (DATE): Ngày cutoff cho backtest
- product_id (STRING), region_id (STRING)
- date (DATE): Ngày thực tế
- horizon (INT): Horizon
- y (DOUBLE): Giá trị thực tế
- yhat (DOUBLE): Giá trị dự báo
- yhat_naive (DOUBLE): Baseline naive (last value)
- run_id (STRING), model_name (STRING), target_type (STRING)

VÍ DỤ:

Câu hỏi: "Doanh thu theo tháng 3 tháng gần đây?"
SQL:
```sql
SELECT 
    date_trunc('month', full_date) AS month,
    SUM(payment_total) AS revenue,
    COUNT(DISTINCT order_id) AS orders
FROM lakehouse.gold.fact_order
WHERE full_date >= date_add('month', -3, CURRENT_DATE)
    AND full_date IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC
LIMIT 200
```

Câu hỏi: "Phương thức thanh toán nào phổ biến nhất?"
SQL:
```sql
SELECT 
    payment_type,
    SUM(orders) AS total_orders,
    SUM(payment_total) AS total_revenue
FROM lakehouse.platinum.dm_payment_mix
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
```

Câu hỏi: "Top 10 sản phẩm bán chạy 6 tháng qua?"
SQL:
```sql
SELECT 
    foi.product_id,
    dp.product_category_name,
    dpc.product_category_name_english,
    COUNT(DISTINCT foi.order_id) AS orders,
    SUM(foi.price) AS revenue
FROM lakehouse.gold.fact_order_item foi
LEFT JOIN lakehouse.gold.dim_product dp ON foi.product_id = dp.product_id
LEFT JOIN lakehouse.gold.dim_product_category dpc ON dp.product_category_name = dpc.product_category_name
WHERE foi.full_date >= date_add('month', -6, CURRENT_DATE)
    AND foi.full_date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 5 DESC
LIMIT 10
```

Câu hỏi: "Tổng forecast doanh thu 7 ngày tới cho từng bang?"
SQL:
```sql
SELECT
    region_id,
    SUM(yhat) AS total_forecast_revenue
FROM lakehouse.platinum.demand_forecast_revenue
WHERE forecast_date >= CURRENT_DATE
  AND forecast_date < date_add('day', 7, CURRENT_DATE)
  AND horizon BETWEEN 1 AND 7
GROUP BY region_id
ORDER BY total_forecast_revenue DESC
LIMIT 200
```

Câu hỏi: "Dự báo nhu cầu 7 ngày tới cho category watches_gifts ở bang SP?"
SQL:
```sql
SELECT
    CAST(f.forecast_date AS DATE) AS forecast_date,
    f.horizon,
    f.yhat,
    f.yhat_lo,
    f.yhat_hi
FROM lakehouse.platinum.demand_forecast_revenue f
LEFT JOIN lakehouse.gold.dim_product dp ON f.product_id = dp.product_id
LEFT JOIN lakehouse.gold.dim_product_category dpc ON dp.product_category_name = dpc.product_category_name
WHERE dpc.product_category_name_english = 'watches_gifts'
  AND f.region_id = 'SP'
  AND CAST(f.forecast_date AS DATE) >= CURRENT_DATE
  AND CAST(f.forecast_date AS DATE) < date_add('day', 7, CURRENT_DATE)
  AND f.horizon BETWEEN 1 AND 7
ORDER BY f.forecast_date, f.horizon
LIMIT 200
```

Câu hỏi: "sMAPE trung bình tháng vừa rồi của model forecast?"
SQL:
```sql
SELECT
    date_trunc('month', date) AS month,
    AVG(smape) AS avg_smape,
    COUNT(*) AS n_samples
FROM lakehouse.platinum.forecast_monitoring
WHERE date >= date_add('month', -1, CURRENT_DATE)
GROUP BY 1
ORDER BY month DESC
LIMIT 200
```

===

CÂU HỎI: ```{question}```

Trả duy nhất câu SQL, không giải thích. Bắt đầu bằng SELECT hoặc WITH.
"""


def gen_sql_with_gemini(question: str) -> str | None:
    """
    Generate SQL query from natural language question using Gemini
    
    Args:
        question: User's question in Vietnamese
        
    Returns:
        SQL query string or None if generation fails
    """
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("⚠️  GOOGLE_API_KEY not set, cannot use Gemini for SQL generation")
        return None
    
    try:
        genai.configure(api_key=api_key)
        
        # Use gemini-2.0-flash for fast, cost-effective SQL generation
        model = genai.GenerativeModel("gemini-2.0-flash")
        
        prompt = PROMPT_SQL.format(question=question)
        
        print(f"🤖 Generating SQL with Gemini for: {question}")
        
        response = model.generate_content(prompt)
        sql = response.text.strip()
        
        # Clean up SQL
        sql = sql.strip("`").strip()
        
        # Remove markdown code blocks if present
        if "```sql" in sql:
            sql = sql.split("```sql")[1].split("```")[0].strip()
        elif "```" in sql:
            sql = sql.split("```")[1].split("```")[0].strip()
        
        # Validate read-only
        if not READONLY_PAT.match(sql):
            print(f"❌ Generated SQL is not read-only: {sql[:100]}")
            return None
        
        # Enforce LIMIT if missing and no GROUP BY
        sql_lower = sql.lower()
        if " limit " not in sql_lower and " group by " not in sql_lower:
            sql = f"{sql.rstrip(';')} LIMIT 200"
        
        # Whitelist check
        if ".gold." not in sql_lower and ".platinum." not in sql_lower:
            print(f"❌ Generated SQL does not use gold/platinum schemas")
            return None
        
        # Check for dangerous keywords
        dangerous = ["delete", "drop", "truncate", "alter", "create", "insert", "update"]
        for keyword in dangerous:
            if keyword in sql_lower:
                print(f"❌ Dangerous keyword '{keyword}' found in SQL")
                return None
        
        print(f"✅ Generated SQL: {sql[:200]}...")
        return sql
        
    except Exception as e:
        print(f"❌ Error generating SQL with Gemini: {e}")
        return None


if __name__ == "__main__":
    # Test SQL generation
    test_questions = [
        "Doanh thu theo tháng 3 tháng gần đây?",
        "Phương thức thanh toán nào phổ biến nhất?",
        "Top 10 sản phẩm bán chạy 6 tháng qua?",
        "Phân bố đơn hàng theo vùng miền?",
        "Tỷ lệ giao hàng đúng hạn theo tuần?"
    ]
    
    print("="*60)
    print("Testing Gemini SQL Generation")
    print("="*60)
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n{i}. Question: {question}")
        sql = gen_sql_with_gemini(question)
        
        if sql:
            print(f"   ✅ SQL:\n{sql}\n")
        else:
            print(f"   ❌ Failed to generate SQL\n")

