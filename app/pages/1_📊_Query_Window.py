import os
import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from trino.dbapi import connect
from trino.auth import BasicAuthentication

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
                return date.fromisoformat(str(min_date)), date.fromisoformat(str(max_date))
        except Exception:
            continue
    
    # Fallback: Brazilian E-commerce (Olist) actual range
    return date(2016, 9, 4), date(2018, 10, 17)

# Get date coverage globally
COVER_MIN, COVER_MAX = get_date_coverage()

def build_sql(catalog:str, schema:str, table:str, date_col:str, grain:str,
              dims:list, measures:list, start:date, end:date, extra_filters:str, 
              use_rollup:bool, use_grouping_sets:bool, limit:int=None):
    """Build SQL query with time grain, dimensions, and measures"""
    
    # Detect if date_col is year_month (varchar) vs full_date (date)
    is_year_month = date_col in ["year_month"]
    
    # Helper function to convert year_month VARCHAR to DATE
    def _month_to_date(col):
        """Convert year_month (YYYY-MM) to DATE for safe comparison"""
        return f"CAST(date_parse({col} || '-01', '%Y-%m-%d') AS date)"
    
    # Helper function to build time expression for SELECT/GROUP/ORDER
    def _time_expr(col, grain_val):
        """Build time expression for grain"""
        if is_year_month:
            # For year_month, parse to date first, then truncate
            month_date = _month_to_date(col)
            return f"date_trunc('{grain_val}', {month_date})"
        else:
            # For date columns, use date_trunc directly
            return f"date_trunc('{grain_val}', {col})"
    
    # Time grain expression
    grain_expr = _time_expr(date_col, grain)
    
    # Build SELECT clause
    select_cols = [f"{grain_expr} AS {grain}"]
    
    # Add dimensions to SELECT
    for dim in dims:
        if "." in dim:  # Handle joined columns
            select_cols.append(dim)
        else:
            select_cols.append(dim)
    
    # Add measures
    select_cols.extend(measures)
    
    # Build GROUP BY clause - use full expressions, not aliases
    group_cols = [grain_expr]  # Use full expression instead of alias
    group_cols.extend(dims)  # Add dimension columns
    
    # Build WHERE clause - date-safe filtering with partition pruning optimization
    if is_year_month:
        # For year_month varchar: add dual predicates for optimization
        # 1. Partition predicate (year_month BETWEEN) for fast pruning
        # 2. Date-safe predicate (parse to DATE) for boundary correctness
        
        # Calculate month range for partition pruning
        start_month = start.strftime("%Y-%m")
        end_month = end.strftime("%Y-%m")
        
        # Calculate next month for date boundary
        if end.month == 12:
            end_next = f"{end.year + 1}-01-01"
        else:
            end_next = f"{end.year}-{end.month + 1:02d}-01"
        
        month_date_expr = _month_to_date(date_col)
        where = [
            # Partition predicate for pruning (fast)
            f"{date_col} >= '{start_month}'",
            f"{date_col} <= '{end_month}'",
            # Date-safe predicate for correctness (precise)
            f"{month_date_expr} >= DATE '{start}'",
            f"{month_date_expr} < DATE '{end_next}'"
        ]
    else:
        # For date columns, use standard date filtering with explicit bounds
        # Use < instead of <= for proper half-open interval
        end_next = (end + timedelta(days=1)).strftime("%Y-%m-%d")
        where = [
            f"CAST({date_col} AS date) >= DATE '{start}'",
            f"CAST({date_col} AS date) < DATE '{end_next}'"
        ]
    
    if extra_filters.strip():
        banned = [";","--","/*","*/","DROP","TRUNCATE","INSERT","UPDATE","DELETE","CALL","CREATE","ALTER"]
        bad = [w for w in banned if w.lower() in extra_filters.lower()]
        if bad:
            raise ValueError(f"Filter chứa từ khóa không hợp lệ: {', '.join(set(bad))}")
        where.append(f"({extra_filters})")
    where_sql = " AND ".join(where)
    
    from_ = f"{catalog}.{schema}.{table}"
    
    # Build GROUP BY with ROLLUP or GROUPING SETS
    if use_grouping_sets and dims:
        sets = []
        # Full dims
        sets.append(f"({grain_expr}, {', '.join(dims)})")
        # Drop dims one by one
        for i in range(len(dims)-1, 0, -1):
            sets.append(f"({grain_expr}, {', '.join(dims[:i])})")
        # Only grain
        sets.append(f"({grain_expr})")
        # Grand total
        sets.append("()")
        group_sql = f"GROUP BY GROUPING SETS (\n  {', '.join(sets)}\n)"
    elif use_rollup and dims:
        group_sql = f"GROUP BY ROLLUP ({grain_expr}, {', '.join(dims)})"
    else:
        group_sql = f"GROUP BY {', '.join(group_cols)}"
    
    # Build ORDER BY - use column positions (1-based) for Trino compatibility
    # Đẩy subtotal/grand total xuống sau cùng bằng cách sort NULLS LAST
    order_positions = [str(i+1) for i in range(1 + len(dims))]  # time grain + dimensions
    order_sql = "ORDER BY " + ", ".join([f"{p} NULLS LAST" for p in order_positions])
    
    # Build LIMIT
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

# Display data coverage info
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
        
        # Display results
        st.success(f"✅ Trả về {len(df):,} dòng")
        st.subheader("📊 Kết quả")
        
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

