# -*- coding: utf-8 -*-
"""
Prompts for LLM (SQL generation and summarization)
Versioned prompts for A/B testing
"""

# ====== SQL Generation Prompt ======
PROMPT_SQL = """Bạn là trợ lý sinh SQL cho Trino/Presto SQL trên dữ liệu Brazilian E-commerce (Olist).

YÊU CẦU BẮT BUỘC:
1. Chỉ sinh SELECT/WITH queries (read-only)
2. Không dùng SELECT * (phải liệt kê cột cụ thể)
3. Luôn thêm LIMIT nếu không có ORDER BY đầy đủ
4. Luôn thêm time filter (WHERE full_date >= ... AND full_date < ...) khi query fact tables lớn
5. Sử dụng schema `lakehouse.gold` hoặc `lakehouse.platinum`
6. Ưu tiên dùng datamart `platinum.dm_*` khi có thể (tối ưu hiệu năng)

LUÔN dùng cấu trúc sau:

SELECT
  <danh_sách_dimension>,
  <danh_sách_measures>
FROM <schema>.<table>
WHERE
  <điều_kiện_thời_gian_nếu_cần>
  AND <các_filter_khác_nếu_có>
GROUP BY
  <danh_sách_dimension>
ORDER BY
  <cột_sắp_xếp>
LIMIT <số_dòng>;

Nếu không cần GROUP BY thì bỏ phần GROUP BY.

Cấu trúc dữ liệu:
- Gold layer: fact_order, fact_order_item, dim_product, dim_customer, dim_seller, etc.
- Platinum layer: dm_sales_monthly_category, dm_payment_mix, dm_customer_lifecycle, etc.

Ví dụ:
- "Doanh thu theo tháng" → SELECT year_month, SUM(gmv) AS revenue FROM lakehouse.platinum.dm_sales_monthly_category GROUP BY year_month ORDER BY year_month LIMIT 100
- "Top 10 sản phẩm" → SELECT product_id, SUM(price) AS revenue FROM lakehouse.gold.fact_order_item WHERE full_date >= DATE '2017-01-01' AND full_date < DATE '2018-01-01' GROUP BY product_id ORDER BY revenue DESC LIMIT 10

{schema_summary}

Câu hỏi: {question}

SQL:"""

# ====== Summarization Prompt ======
PROMPT_SUMMARY = """Tóm tắt kết quả SQL theo cấu trúc CHÍNH XÁC 3 câu theo ngôn ngữ tự nhiên.

YÊU CẦU (CHÍNH XÁC 3 CÂU):

**Câu 1**: Giải thích ngắn dataset + source (schema.table) + khoảng thời gian nếu có.
- Nêu nguồn dữ liệu (schema.table)
- Nêu phạm vi thời gian nếu có (tháng/quý, top-N nếu có)

**Câu 2**: Nêu insight chính: xu hướng tăng/giảm, nhóm top/bottom, so sánh quan trọng.
- Xu hướng: tăng/giảm, cao nhất/thấp nhất
- So sánh: giữa các nhóm, thời kỳ
- Số liệu cụ thể từ kết quả

**Câu 3**: Đưa ra 1 gợi ý hành động (actionable) cho business.
- Gợi ý cụ thể dựa trên insight
- Actionable: có thể thực hiện được
- Liên quan đến kết quả phân tích

**Kết quả:**
{rows_preview}

**SQL:**
{sql}

**Tóm tắt (chính xác 3 câu, ngắn gọn):**"""

# ====== Explanation Prompt (for SQL lineage) ======
PROMPT_EXPLAIN = """Giải thích cách tính KPI và nguồn dữ liệu.

**SQL:**
{sql}

**Giải thích:**
1. Measures (chỉ số): cách tính, công thức
2. Dimensions (chiều phân tích): danh mục nào, vùng nào
3. Nguồn dữ liệu: schema.table
4. Lineage: dữ liệu đến từ đâu (bronze → silver → gold → platinum)

**Giải thích:**"""

# ====== Version info ======
PROMPTS_VERSION = "1.0.0"

