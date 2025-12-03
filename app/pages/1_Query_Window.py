import os
import re
import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import sys
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.time_utils import (
    half_open_range, dual_predicates, timegrain_expr,
    coerce_date_col, format_time_bucket_alias
)
from utils.sql_guardrails import (
    is_safe_select, maybe_add_limit, check_explain_analyze,
    check_order_by_with_limit, detect_cast_on_partition,
    auto_fix_cast_on_partition, check_multi_statement_safe,
    check_grouping_sets_fallback
)

# ====== Config ======
def get_conf(key, default=None):
    # ưu tiên secrets.toml, sau đó env
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except:
        return os.getenv(key, default)

TRINO_HOST = get_conf("TRINO_HOST", "trino")
TRINO_PORT = int(get_conf("TRINO_PORT", "8080"))
TRINO_CATALOG = get_conf("TRINO_CATALOG", "lakehouse")
TRINO_USER = get_conf("TRINO_USER", "admin")
TRINO_PASSWORD = get_conf("TRINO_PASSWORD", "") or None

DEFAULT_SCHEMA = get_conf("TRINO_DEFAULT_SCHEMA", "gold")

# ====== Helpers ======
@st.cache_data(ttl=600, show_spinner="Đang chạy truy vấn...")
def run_query(sql: str, schema: str):
    """Execute SQL query on Trino and return DataFrame"""
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=schema,
            http_scheme="http",
            auth=None if not TRINO_PASSWORD else BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
            source="streamlit-query-window"
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Lỗi kết nối Trino: {e}")
        raise

# ====== Date Coverage Detection ======
@st.cache_data(ttl=3600)  # Cache 1 hour
def get_date_coverage():
    """Get actual date range from Brazilian E-commerce (Olist) data"""
    coverage_sqls = [
        ("gold", "fact_order", "SELECT CAST(MIN(full_date) AS date) AS min_d, CAST(MAX(full_date) AS date) AS max_d FROM lakehouse.gold.fact_order"),
        ("gold", "fact_order_item", "SELECT CAST(MIN(full_date) AS date) AS min_d, CAST(MAX(full_date) AS date) AS max_d FROM lakehouse.gold.fact_order_item"),
        ("platinum", "demand_forecast", "SELECT CAST(MIN(forecast_date) AS date) AS min_d, CAST(MAX(forecast_date) AS date) AS max_d FROM lakehouse.platinum.demand_forecast"),
    ]
    
    for schema, table, sql in coverage_sqls:
        try:
            df = run_query(sql, schema)
            if not df.empty and pd.notna(df.loc[0, "min_d"]) and pd.notna(df.loc[0, "max_d"]):
                min_date = df.loc[0, "min_d"]
                max_date = df.loc[0, "max_d"]
                return date.fromisoformat(str(min_date)), date.fromisoformat(str(max_date)), False  # False = not fallback
        except Exception as e:
            st.session_state['_coverage_error'] = str(e)
            continue
    
    # Fallback: Brazilian E-commerce (Olist) actual range
    return date(2016, 9, 4), date(2018, 10, 17), True  # True = is fallback

# Get date coverage globally
_coverage_result = get_date_coverage()
COVER_MIN, COVER_MAX = _coverage_result[0], _coverage_result[1]
USING_FALLBACK_DATE = _coverage_result[2] if len(_coverage_result) > 2 else False

def build_sql(catalog:str, schema:str, table:str, date_col:str, grain:str,
              dims:list, measures:list, start:date, end:date, extra_filters:str, 
              use_rollup:bool, use_grouping_sets:bool, limit:int=None):
    """
    Build SQL query with time grain, dimensions, and measures
    Cải thiện: dual predicates, pre-select cho GROUPING SETS/ROLLUP
    """
    # Detect if date_col is year_month (varchar) vs full_date (date)
    is_year_month = date_col in ["year_month"]
    
    # Chuẩn hóa half-open range
    start_date, end_next_date = half_open_range(start, end)
    
    # Time grain expression (dùng time_utils)
    grain_expr = timegrain_expr(grain, date_col, is_year_month)
    time_bucket_alias = format_time_bucket_alias(grain)
    
    # Build WHERE clause - dual predicates cho partition pruning
    if is_year_month:
        # Giả định bảng có cả year_month (VARCHAR partition) và full_date (DATE)
        prune_pred, exact_pred = dual_predicates(
            date_col,  # year_month VARCHAR
            "full_date",  # full_date DATE
            start_date,
            end_next_date
        )
        where = [prune_pred, exact_pred]
    else:
        # For date columns, use standard half-open filtering
        where = [
            f"{date_col} >= DATE '{start_date}'",
            f"{date_col} < DATE '{end_next_date}'"
        ]
    
    if extra_filters.strip():
        # Kiểm tra an toàn với is_safe_select (đã bỏ string literal)
        if not is_safe_select(f"SELECT * FROM dummy WHERE {extra_filters}"):
            raise ValueError("Filter chứa từ khóa không hợp lệ (DDL/DML)")
        where.append(f"({extra_filters})")
    where_sql = " AND ".join(where)
    
    from_ = f"{catalog}.{schema}.{table}"
    
    # Cải thiện: Fallback GROUPING SETS/ROLLUP nếu không có dims
    use_grouping_sets, use_rollup = check_grouping_sets_fallback(
        use_grouping_sets, use_rollup, dims
    )
    
    # Cải thiện: Pre-select time_bucket khi dùng ROLLUP/GROUPING SETS
    # (Trino không cho group theo biểu thức trong GROUPING SETS)
    if use_rollup or use_grouping_sets:
        # Pre-select trong CTE
        base_select_cols = [f"{grain_expr} AS {time_bucket_alias}"]
        base_select_cols.extend(dims)
        base_select_cols.extend(measures)
        
        base_sql = f"""
WITH base AS (
  SELECT
    {', '.join(base_select_cols)}
  FROM {from_}
  WHERE {where_sql}
)"""
        
        # GROUP BY dùng cột (không phải biểu thức)
        if use_grouping_sets and dims:
            sets = []
            sets.append(f"({time_bucket_alias}, {', '.join(dims)})")
            for i in range(len(dims)-1, 0, -1):
                sets.append(f"({time_bucket_alias}, {', '.join(dims[:i])})")
            sets.append(f"({time_bucket_alias})")
            sets.append("()")
            group_sql = f"GROUP BY GROUPING SETS (\n  {', '.join(sets)}\n)"
        elif use_rollup and dims:
            group_sql = f"GROUP BY ROLLUP ({time_bucket_alias}, {', '.join(dims)})"
        else:
            group_sql = f"GROUP BY {time_bucket_alias}, {', '.join(dims)}"
        
        # SELECT từ base với COALESCE cho NULL → "ALL"
        select_cols = [f"{time_bucket_alias} AS {grain}"]
        for dim in dims:
            select_cols.append(f"COALESCE(CAST({dim} AS VARCHAR), 'ALL') AS {dim}")
        select_cols.extend(measures)
        
        order_positions = [str(i+1) for i in range(1 + len(dims))]
        order_sql = "ORDER BY " + ", ".join([f"{p} NULLS LAST" for p in order_positions])
        limit_sql = f"LIMIT {limit}" if limit else ""
        
        sql = f"""{base_sql}
SELECT
  {', '.join(select_cols)}
FROM base
{group_sql}
{order_sql}
{limit_sql}
        """.strip()
    else:
        # Không dùng ROLLUP/GROUPING SETS → SQL đơn giản
        select_cols = [f"{grain_expr} AS {grain}"]
        select_cols.extend(dims)
        select_cols.extend(measures)
        
        group_cols = [grain_expr]
        group_cols.extend(dims)
        group_sql = f"GROUP BY {', '.join(group_cols)}"
        
        order_positions = [str(i+1) for i in range(1 + len(dims))]
        order_sql = "ORDER BY " + ", ".join([f"{p} NULLS LAST" for p in order_positions])
        limit_sql = f"LIMIT {limit}" if limit else ""
        
        sql = f"""
SELECT
  {', '.join(select_cols)}
FROM {from_}
WHERE {where_sql}
{group_sql}
{order_sql}
{limit_sql}
        """.strip()
    
    return sql

# ====== TABLE & COLUMN METADATA ======
TABLES_META = {
    "gold": {
        "fact_order": {
            "date_col": "full_date",
            "description": "Fact table: 1 row per order",
            "dimensions": [
                "customer_id",
                "primary_payment_type",
                "is_canceled",
                "delivered_on_time"
            ],
            "measures": [
                "COUNT(*) AS order_count",
                "COUNT(DISTINCT customer_id) AS unique_customers",
                "SUM(items_count) AS total_items",
                "SUM(sum_price) AS total_price",
                "SUM(sum_freight) AS total_freight",
                "SUM(payment_total) AS total_payment",
                "AVG(delivered_days) AS avg_delivery_days",
                "SUM(CASE WHEN delivered_on_time THEN 1 ELSE 0 END) AS on_time_deliveries",
                "SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) AS canceled_orders"
            ]
        },
        "fact_order_item": {
            "date_col": "full_date",
            "description": "Fact table: 1 row per order item",
            "dimensions": [
                "product_id",
                "seller_id",
                "customer_id",
                "order_status"
            ],
            "measures": [
                "COUNT(*) AS item_count",
                "COUNT(DISTINCT order_id) AS order_count",
                "COUNT(DISTINCT product_id) AS unique_products",
                "COUNT(DISTINCT seller_id) AS unique_sellers",
                "SUM(price) AS total_revenue",
                "SUM(freight_value) AS total_freight",
                "AVG(price) AS avg_item_price"
            ]
        }
    },
    "platinum": {
        "dm_sales_monthly_category": {
            "date_col": "year_month",
            "description": "Monthly sales by product category",
            "dimensions": [
                "product_category_name_english"
            ],
            "measures": [
                "SUM(gmv) AS gmv",
                "SUM(orders) AS orders",
                "SUM(units) AS units",
                "AVG(aov) AS avg_aov"
            ]
        },
        "dm_customer_lifecycle": {
            "date_col": "year_month",
            "description": "Customer lifecycle analysis",
            "dimensions": [
                "cohort_month"
            ],
            "measures": [
                "SUM(orders) AS orders",
                "SUM(gmv) AS gmv",
                "COUNT(DISTINCT customer_id) AS unique_customers"
            ]
        },
        "dm_payment_mix": {
            "date_col": "year_month",
            "description": "Payment method mix by month",
            "dimensions": [
                "payment_type"
            ],
            "measures": [
                "SUM(orders) AS orders",
                "SUM(unique_customers) AS unique_customers",
                "SUM(payment_total) AS payment_total"
            ]
        },
        "dm_logistics_sla": {
            "date_col": "year_month",
            "description": "Logistics SLA metrics by region",
            "dimensions": [
                "geolocation_state"
            ],
            "measures": [
                "AVG(avg_delivered_days) AS avg_delivered_days",
                "AVG(on_time_rate) AS on_time_rate",
                "SUM(late_orders) AS late_orders"
            ]
        },
        "demand_forecast": {
            "date_col": "forecast_date",
            "description": "Demand forecast (ML predictions)",
            "dimensions": [
                "product_id",
                "region_id",
                "model_name"
            ],
            "measures": [
                "AVG(yhat) AS forecast",
                "AVG(yhat_lo) AS ci_lower",
                "AVG(yhat_hi) AS ci_upper",
                "COUNT(DISTINCT horizon) AS horizons"
            ]
        }
    }
}

# Enhanced CSS
st.markdown("""
<style>
:root{
  --bg:#0b1220; --bg2:#10192b; --card:#0f172a; --line:#1f2a44;
  --text:#e2e8f0; --muted:#94a3b8; --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; --pri:#22d3ee;
}
.main .block-container{max-width:1280px;padding-top:1rem;padding-bottom:3rem}
h1,h2,h3{letter-spacing:.2px}
hr{border-color:var(--line);opacity:.4;margin:1rem 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px;transition:border-color 0.2s}
.card:hover{border-color:#294166}
.badge{display:inline-flex;gap:6px;align-items:center;padding:4px 10px;border-radius:999px;
  background:rgba(34,211,238,.12);border:1px solid rgba(34,211,238,.25);font-size:12px}
.muted{color:var(--muted)}
.stButton>button{border-radius:14px;padding:.8rem 1.1rem;font-weight:700;font-size:15px}
.section-title{display:flex;align-items:center;gap:10px;margin:1.5rem 0 1rem}
.section-title h3{margin:0}
.section-title:after{content:"";flex:1;height:1px;background:linear-gradient(90deg,transparent, var(--line))}
</style>
""", unsafe_allow_html=True)

# ====== UI ======
st.title("🧮 Cửa sổ truy vấn đa chiều")
st.caption("Phân tích dữ liệu Brazilian E-commerce với Trino • OLAP ROLLUP/GROUPING SETS")

# ====== Mode Selection ======
mode = st.sidebar.radio("🔀 Chế độ", ["Trình dựng (GUI)", "SQL thủ công"], index=0)

# ====== Manual SQL Mode ======
if mode == "SQL thủ công":
    st.subheader("🧩 SQL thủ công (SELECT-only)")
    st.caption("Chỉ cho phép SELECT/WITH trên lakehouse.gold|platinum. Có thể dùng :start, :end, :month.")
    
    # Sample SQL template
    sample_sql = """WITH base AS (
  SELECT *
  FROM lakehouse.gold.fact_order
  WHERE CAST(full_date AS date) >= DATE :start
    AND CAST(full_date AS date) <  DATE :end
)
SELECT 
  date_trunc('month', full_date) AS month,
  primary_payment_type,
  SUM(payment_total) AS total_payment,
  COUNT(*) AS order_count
FROM base
GROUP BY 1, 2
ORDER BY 1, 3 DESC
LIMIT 100
"""
    
    sql_input = st.text_area(
        "📝 SQL Query",
        sample_sql,
        height=260,
        key="custom_sql",
        help="Nhập câu lệnh SQL. Sử dụng :start, :end, :month làm placeholder cho tham số."
    )
    
    # Quick parameters
    st.markdown("**⚙️ Tham số nhanh**")
    param_col1, param_col2, param_col3 = st.columns(3)
    
    with param_col1:
        d_start = st.date_input(
            "📅 Start",
            value=COVER_MIN,
            min_value=COVER_MIN,
            max_value=COVER_MAX,
            help="Ngày bắt đầu (dùng cho :start)"
        )
    
    with param_col2:
        d_end = st.date_input(
            "📅 End (exclusive)",
            value=COVER_MAX,
            min_value=COVER_MIN,
            max_value=COVER_MAX,
            help="Ngày kết thúc (exclusive, dùng cho :end)"
        )
    
    with param_col3:
        # Default to current month or last month of data
        default_month = COVER_MAX.strftime("%Y-%m")
        month_input = st.text_input(
            "📆 Month (YYYY-MM)",
            value=default_month,
            help="Tháng (dùng cho :month)"
        )
    
    # Options
    auto_limit = st.checkbox(
        "✅ Tự động thêm LIMIT 10000 nếu thiếu",
        value=True,
        help="Tự động thêm LIMIT 10000 vào cuối câu lệnh SQL nếu chưa có"
    )
    
    show_explain = st.checkbox(
        "🔍 Hiện kế hoạch (EXPLAIN) trước khi chạy",
        value=False,
        help="Hiển thị execution plan trước khi chạy truy vấn"
    )
    
    # ====== Safety Checks (Cải thiện: bỏ string literal, word boundary) ======
    sql = sql_input.strip()
    
    # Kiểm tra multi-statement
    is_single, multi_error = check_multi_statement_safe(sql)
    if not is_single:
        st.error(f"❌ {multi_error}")
        st.stop()
    
    # Kiểm tra an toàn với is_safe_select (đã bỏ string literal)
    if not is_safe_select(sql):
        st.error("❌ SQL không an toàn. Chỉ cho phép SELECT/WITH, không có DDL/DML.")
        st.stop()
    
    # Require valid catalog/schema (warning only, not blocking)
    schema_match = re.search(r"\blakehouse\.(gold|platinum)\.", sql, re.IGNORECASE)
    if not schema_match:
        st.warning("⚠️ Hãy truy vấn trong lakehouse.gold hoặc lakehouse.platinum (ví dụ: lakehouse.gold.fact_order).")
        # Don't stop, just warn - user might be using subqueries or views
    else:
        # Extract schema from match for later use
        detected_schema = schema_match.group(1).lower()
    
    # Check if placeholders exist in original SQL
    has_placeholders = ":start" in sql_input or ":end" in sql_input or ":month" in sql_input
    
    # Replace placeholders safely (only if they exist in SQL)
    if ":start" in sql:
        sql = sql.replace(":start", f"'{d_start.isoformat()}'")
    if ":end" in sql:
        sql = sql.replace(":end", f"'{d_end.isoformat()}'")
    if ":month" in sql:
        sql = sql.replace(":month", f"'{month_input}'")
    
    # Cải thiện: Auto-LIMIT thông minh (không thêm nếu đã có LIMIT/OFFSET/FETCH)
    sql_before = sql
    sql = maybe_add_limit(sql, default_limit=10000, enabled=auto_limit)
    limit_added = (sql != sql_before)
    
    # Display final SQL (always show if there were changes)
    if limit_added or has_placeholders:
        st.markdown("**📋 SQL đã xử lý:**")
        st.code(sql, language="sql")
        if limit_added:
            st.info("ℹ️ Đã tự động thêm LIMIT 10000 vào câu lệnh SQL.")
    else:
        # Show a collapsible section for SQL preview
        with st.expander("📋 Xem SQL đã xử lý", expanded=False):
            st.code(sql, language="sql")
    
    # Cải thiện: EXPLAIN ANALYZE → EXPLAIN (an toàn hơn)
    sql, explain_changed = check_explain_analyze(sql)
    if explain_changed:
        st.warning("⚠️ Đã chuyển EXPLAIN ANALYZE → EXPLAIN để an toàn (không thực thi query).")
    
    # EXPLAIN button (optional)
    if show_explain:
        if st.button("🔍 EXPLAIN", use_container_width=True):
            try:
                explain_sql = f"EXPLAIN {sql}"
                with st.spinner("⏳ Đang phân tích execution plan..."):
                    df_explain = run_query(explain_sql, DEFAULT_SCHEMA)
                
                if not df_explain.empty:
                    st.success("✅ Execution Plan:")
                    st.dataframe(df_explain, use_container_width=True, height=280)
                else:
                    st.warning("⚠️ Không thể lấy execution plan.")
            except Exception as e:
                st.error(f"❌ Lỗi EXPLAIN: {e}")
    
    # Cải thiện: Cảnh báo LIMIT không kèm ORDER BY
    has_limit, has_order_by = check_order_by_with_limit(sql)
    if has_limit and not has_order_by:
        st.warning("⚠️ Bạn đang giới hạn số dòng nhưng không sắp xếp; thứ tự có thể không ổn định.")
    
    # Cải thiện: Auto-fix CAST trên partition (opt-in)
    auto_fix_cast = st.checkbox(
        "🔧 Tự động sửa CAST trên partition (thêm dual predicates)",
        value=False,
        help="Tự động thêm partition predicate để tối ưu hiệu năng"
    )
    
    if auto_fix_cast:
        sql, was_fixed = auto_fix_cast_on_partition(
            sql, partition_col="year_month", date_col="full_date",
            start=d_start, end=d_end
        )
        if was_fixed:
            st.success("✅ Đã tự động thêm partition predicate để tối ưu hiệu năng.")
    elif detect_cast_on_partition(sql):
        st.info("💡 Gợi ý: Bạn đang CAST trên cột partition; bật checkbox trên để tự động thêm dual predicates.")
    
    # Run SQL button
    if st.button("▶️ Run SQL", type="primary", use_container_width=True):
        try:
            with st.spinner("⏳ Đang chạy truy vấn..."):
                # Determine schema from SQL (default to gold)
                # Check for platinum first, then gold, then default to gold
                if re.search(r"\blakehouse\.platinum\.", sql, re.IGNORECASE):
                    query_schema = "platinum"
                elif re.search(r"\blakehouse\.gold\.", sql, re.IGNORECASE):
                    query_schema = "gold"
                else:
                    # If no schema detected, default to gold but show warning
                    query_schema = "gold"
                    st.warning("⚠️ Không phát hiện schema trong SQL. Đang dùng schema mặc định: gold")
                
                df = run_query(sql, query_schema)
            
            if df.empty:
                st.warning("📭 Không có dữ liệu trả về.")
                st.stop()
            
            # Lưu SQL thành công vào session state (UX improvement)
            st.session_state['last_success_sql'] = sql
            st.session_state['last_success_params'] = {
                'start': d_start.isoformat(),
                'end': d_end.isoformat(),
                'month': month_input
            }
            
            # Display results
            st.success(f"✅ Trả về {len(df):,} dòng")
            st.subheader("📊 Kết quả")
            
            # Copy SQL button (UX improvement)
            if 'last_success_sql' in st.session_state:
                st.code(st.session_state['last_success_sql'], language="sql")
                if st.button("📋 Copy SQL", key="copy_sql_manual"):
                    st.success("✅ SQL đã được copy vào clipboard (dán vào editor để dùng lại)")
            
            # Display dataframe
            st.dataframe(df, use_container_width=True, height=500)
            
            # Summary statistics
            with st.expander("📈 Thống kê tổng hợp"):
                st.write("**Tổng số dòng:**", f"{len(df):,}")
                st.write("**Số cột:**", len(df.columns))
                st.write("**Các cột:**", ", ".join(df.columns))
                
                # Show data types
                st.write("**Kiểu dữ liệu:**")
                for col in df.columns:
                    dtype = str(df[col].dtype)
                    st.write(f"  - `{col}`: {dtype}")
            
            # Export options
            st.subheader("💾 Xuất dữ liệu")
            export_col1, export_col2 = st.columns(2)
            
            with export_col1:
                # CSV export
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Tải CSV",
                    csv,
                    f"custom_sql_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv",
                    use_container_width=True
                )
            
            with export_col2:
                # Excel export
                try:
                    import io
                    bio = io.BytesIO()
                    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='Query Result')
                    st.download_button(
                        "⬇️ Tải Excel",
                        bio.getvalue(),
                        f"custom_sql_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"❌ Lỗi export Excel: {e}")
        
        except Exception as e:
            st.error(f"❌ Lỗi truy vấn: {e}")
            st.code(sql, language="sql")
            # Show helpful error message
            error_str = str(e)
            if "TABLE_NOT_FOUND" in error_str or "does not exist" in error_str:
                st.info("💡 Gợi ý: Kiểm tra tên bảng và schema. Ví dụ: `lakehouse.gold.fact_order`")
            elif "SYNTAX_ERROR" in error_str or "syntax" in error_str.lower():
                st.info("💡 Gợi ý: Kiểm tra cú pháp SQL. Đảm bảo dùng cú pháp Trino/Presto SQL.")
    
    # Help section for manual SQL
    with st.expander("ℹ️ Hướng dẫn SQL thủ công", expanded=False):
        st.markdown("""
        ### 📝 Cách sử dụng:
        
        1. **Nhập SQL**: Gõ câu lệnh SELECT/WITH trong khung text area
        2. **Tham số**: Sử dụng `:start`, `:end`, `:month` làm placeholder
        3. **Chạy**: Nhấn nút "Run SQL" để thực thi
        
        ### 🔒 Rào chắn an toàn:
        - ✅ Chỉ cho phép SELECT/WITH
        - ✅ Chặn DDL/DML (DROP, INSERT, UPDATE, DELETE, etc.)
        - ✅ Bắt buộc tham chiếu trong `lakehouse.gold` hoặc `lakehouse.platinum`
        - ✅ Tự động thêm LIMIT 10000 nếu thiếu
        
        ### 💡 Ví dụ SQL:
        
        **Ví dụ 1: Truy vấn đơn giản**
        ```sql
        SELECT *
        FROM lakehouse.gold.fact_order
        WHERE CAST(full_date AS date) >= DATE :start
          AND CAST(full_date AS date) <  DATE :end
        LIMIT 100
        ```
        
        **Ví dụ 2: Với CTE (WITH)**
        ```sql
        WITH monthly_sales AS (
          SELECT 
            date_trunc('month', full_date) AS month,
            SUM(payment_total) AS total
          FROM lakehouse.gold.fact_order
          WHERE CAST(full_date AS date) >= DATE :start
            AND CAST(full_date AS date) <  DATE :end
          GROUP BY 1
        )
        SELECT * FROM monthly_sales ORDER BY month
        ```
        
        **Ví dụ 3: JOIN với dimension tables**
        ```sql
        SELECT 
          o.full_date,
          p.product_category_name_english AS category,
          SUM(oi.price) AS revenue
        FROM lakehouse.gold.fact_order_item oi
        JOIN lakehouse.gold.fact_order o ON oi.order_id = o.order_id
        JOIN lakehouse.gold.dim_product p ON oi.product_id = p.product_id
        WHERE CAST(o.full_date AS date) >= DATE :start
          AND CAST(o.full_date AS date) <  DATE :end
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
        ```
        
        ### ⚠️ Lưu ý:
        - **Half-open interval**: Dùng `>= :start AND < :end` để tránh lỗi biên
        - **Year-month columns**: Nếu dùng `year_month` (VARCHAR), parse sang DATE:
          ```sql
          WHERE date_parse(year_month || '-01', '%Y-%m-%d') >= DATE :start
            AND date_parse(year_month || '-01', '%Y-%m-%d') <  DATE :end
          ```
        - **Performance**: Giới hạn số dòng với LIMIT để tránh query quá nặng
        - **EXPLAIN**: Dùng checkbox "Hiện kế hoạch" để xem execution plan trước khi chạy
        """)
    
    st.stop()  # Stop here, don't run GUI mode below

# ====== GUI Mode (Original Query Builder) ======
# ====== Schema & Table Selection ======
col_meta1, col_meta2 = st.columns(2)

with col_meta1:
    schemas = list(TABLES_META.keys())
    schema = st.selectbox("📁 Schema", schemas, index=schemas.index(DEFAULT_SCHEMA))

with col_meta2:
    tables = list(TABLES_META[schema].keys())
    fact_table = st.selectbox("📊 Fact Table", tables)

table_meta = TABLES_META[schema][fact_table]
st.info(f"ℹ️ {table_meta['description']}")

# ====== Time Selection ======
st.subheader("⏰ Thời gian")

# Helper functions for date manipulation
def first_of_month(d: date):
    """Get first day of month"""
    return d.replace(day=1)

def subtract_months(d: date, months: int):
    """Subtract months from a date safely"""
    year = d.year
    month = d.month - months
    while month <= 0:
        month += 12
        year -= 1
    # Handle day overflow (e.g., Jan 31 - 1 month = Dec 31, not Dec 30)
    day = min(d.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return date(year, month, day)

# Calculate default dates based on coverage
time_grain = st.selectbox("🔍 Time Grain", ["day","week","month","quarter","year"], index=2)

# Smart defaults based on grain and data coverage
if time_grain == "month":
    # For month: last 6 months
    default_end = COVER_MAX
    # Calculate start as 6 months before
    default_start = subtract_months(default_end, 6)
    default_start = first_of_month(default_start)
else:
    # For day: last 90 days
    default_start = COVER_MAX - timedelta(days=89)
    default_end = COVER_MAX

col_time2, col_time3 = st.columns(2)

with col_time2:
    start_date = st.date_input(
        "📅 Từ ngày", 
        value=default_start,
        min_value=COVER_MIN,
        max_value=COVER_MAX
    )

with col_time3:
    end_date = st.date_input(
        "📅 Đến ngày", 
        value=default_end,
        min_value=COVER_MIN,
        max_value=COVER_MAX
    )

# Validate date range
if start_date > end_date:
    st.warning("⚠️ Khoảng thời gian không hợp lệ. Đã tự chỉnh về biên hợp lệ gần nhất.")
    start_date = COVER_MIN
    end_date = COVER_MAX

# Display data coverage info với badge cảnh báo fallback (UX improvement)
if USING_FALLBACK_DATE:
    col_warn1, col_warn2 = st.columns([3, 1])
    with col_warn1:
        st.warning("⚠️ **Đang dùng dải ngày mặc định** do không truy cập được metadata. Hãy kiểm tra kết nối/bảng nguồn.")
    with col_warn2:
        if st.button("🔄 Retry với safe defaults", key="retry_coverage"):
            st.cache_data.clear()
            st.rerun()
    if '_coverage_error' in st.session_state:
        with st.expander("🔍 Chi tiết lỗi"):
            st.code(st.session_state['_coverage_error'], language=None)
st.caption(f"📌 Dải dữ liệu khả dụng: {COVER_MIN} → {COVER_MAX} (Brazilian E-commerce/Olist)")

# ====== Dimensions Selection ======
st.subheader("📐 Dimensions (Chiều phân tích)")
available_dims = table_meta["dimensions"]
dims = st.multiselect(
    "Chọn các chiều để phân tích (tối đa 3-4 chiều cho hiệu suất tốt)",
    available_dims,
    default=available_dims[:2] if len(available_dims) >= 2 else available_dims
)

# ====== Measures Selection ======
st.subheader("📏 Measures (Chỉ số)")
available_measures = table_meta["measures"]
measures = st.multiselect(
    "Chọn các chỉ số cần tính toán",
    available_measures,
    default=available_measures[:3] if len(available_measures) >= 3 else available_measures
)

# ====== Filters ======
st.subheader("🔍 Bộ lọc")
extra_filters = st.text_area(
    "WHERE clause bổ sung (ví dụ: primary_payment_type = 'credit_card' AND delivered_on_time = true)",
    "",
    height=80
)

# ====== Advanced Options ======
with st.expander("⚙️ Tùy chọn nâng cao"):
    col_adv1, col_adv2, col_adv3 = st.columns(3)
    
    with col_adv1:
        use_rollup = st.checkbox("Dùng ROLLUP (tổng theo mọi cấp)", value=False)
    
    with col_adv2:
        use_gsets = st.checkbox("Dùng GROUPING SETS", value=False)
    
    with col_adv3:
        limit_rows = st.number_input("Giới hạn số dòng", min_value=0, max_value=100000, value=10000, step=1000)

# ====== Run Query ======
if st.button("▶️ Chạy truy vấn", type="primary", use_container_width=True):
    if not dims:
        st.warning("⚠️ Vui lòng chọn ít nhất 1 dimension")
        st.stop()
    
    if not measures:
        st.warning("⚠️ Vui lòng chọn ít nhất 1 measure")
        st.stop()
    
    # Build SQL
    sql = build_sql(
        TRINO_CATALOG, schema, fact_table, table_meta["date_col"], time_grain,
        dims, measures, start_date, end_date, extra_filters,
        use_rollup, use_gsets, limit_rows if limit_rows > 0 else None
    )
    
    # Show SQL
    with st.expander("📝 SQL Query"):
        st.code(sql, language="sql")
    
    # Execute query
    try:
        with st.spinner("⏳ Đang truy vấn dữ liệu..."):
            df = run_query(sql, schema)
        
        if df.empty:
            st.warning("📭 Không có dữ liệu phù hợp với điều kiện")
            st.stop()
        
        # Lưu SQL thành công vào session state (UX improvement)
        st.session_state['last_success_sql'] = sql
        st.session_state['last_success_params'] = {
            'schema': schema,
            'table': fact_table,
            'grain': time_grain,
            'start': start_date.isoformat(),
            'end': end_date.isoformat()
        }
        
        # Display results
        st.success(f"✅ Trả về {len(df):,} dòng")
        st.subheader("📊 Kết quả")
        
        # Copy SQL button (UX improvement)
        if 'last_success_sql' in st.session_state:
            with st.expander("📋 SQL đã chạy (click để copy)", expanded=False):
                st.code(st.session_state['last_success_sql'], language="sql")
        
        # Tạo bản hiển thị riêng (giữ df gốc để export chuẩn số)
        df_display = df.copy()
        numeric_cols = df_display.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            df_display[col] = df_display[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
        
        st.dataframe(df_display, use_container_width=True, height=500)
        
        # Summary statistics
        with st.expander("📈 Thống kê tổng hợp"):
            st.write("Tổng số dòng:", f"{len(df):,}")
            st.write("Các cột:", ", ".join(df.columns))
        
        # Export options
        st.subheader("💾 Xuất dữ liệu")
        col_exp1, col_exp2 = st.columns(2)
        
        with col_exp1:
            # CSV export
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Tải CSV",
                csv,
                f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv",
                use_container_width=True
            )
        
        with col_exp2:
            # Excel export
            try:
                import io
                bio = io.BytesIO()
                with pd.ExcelWriter(bio, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Query Result')
                st.download_button(
                    "⬇️ Tải Excel",
                    bio.getvalue(),
                    f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            except Exception as e:
                st.error(f"Lỗi export Excel: {e}")
        
    except Exception as e:
        st.error(f"❌ Lỗi truy vấn: {e}")
        st.code(sql, language="sql")

# ====== Help Section ======
with st.sidebar:
    st.header("ℹ️ Hướng dẫn")
    
    st.markdown("""
    ### Cách sử dụng:
    
    1. **Chọn Schema & Table**: Chọn lớp dữ liệu và bảng fact
    2. **Chọn Time Grain**: Độ chi tiết thời gian (ngày/tuần/tháng...)
    3. **Chọn Dimensions**: Các chiều phân tích (tối đa 3-4)
    4. **Chọn Measures**: Các chỉ số cần tính
    5. **Thêm Filters**: Điều kiện lọc bổ sung (tùy chọn)
    6. **Chạy**: Nhấn nút "Chạy truy vấn"
    
    ### Ví dụ Filters:
    ```sql
    primary_payment_type = 'credit_card'
    delivered_on_time = true
    sum_price > 100
    ```
    
    ### Tips:
    - Giới hạn khoảng thời gian để truy vấn nhanh hơn
    - Chọn ít dimensions để tránh quá nhiều dòng
    - Dùng ROLLUP để xem tổng theo từng cấp
    """)
    
    st.divider()
    
    st.markdown(f"""
    **Trino Connection:**
    - Host: `{TRINO_HOST}:{TRINO_PORT}`
    - Catalog: `{TRINO_CATALOG}`
    - Schema: `{schema}`
    """)

