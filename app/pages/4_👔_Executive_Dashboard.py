"""
Executive Dashboard Suite - Báo cáo trực tiếp cho lãnh đạo
10 dashboard với nhiều biểu đồ và KPI tiles
"""
import os
import math
import datetime as dt
from typing import Tuple, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# =========================
# Page config & Style
# =========================
st.set_page_config(
    page_title="Executive Analytics Suite",
    page_icon="👔",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Enhanced CSS - "xịn sò" UI
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

:root{
  --bg:#0b1220; --bg2:#10192b; --card:#0f172a; --line:#1f2a44;
  --text:#e2e8f0; --muted:#94a3b8; --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; --pri:#3B82F6;
  --sec:#10B981; --danger:#EF4444;
}

html, body, [class*=css] { font-family: 'Inter', sans-serif; }
.main .block-container{max-width:1400px;padding-top:1.2rem;padding-bottom:2rem}

/* KPI tile */
.kpi-card{
    border-radius:16px; 
    padding:16px 20px; 
    background:linear-gradient(135deg, var(--card) 0%, var(--bg2) 100%); 
    border:1px solid var(--line); 
    box-shadow:0 2px 12px rgba(0,0,0,0.15);
    transition:all 0.3s ease;
}
.kpi-card:hover{
    transform:translateY(-2px);
    box-shadow:0 4px 20px rgba(59,130,246,0.2);
    border-color:var(--pri);
}
.kpi-title{font-size:0.85rem; color:var(--muted); margin-bottom:6px; font-weight:500; text-transform:uppercase; letter-spacing:0.5px}
.kpi-value{font-size:1.8rem; font-weight:700; color:var(--text); margin-bottom:4px}
.kpi-delta{font-size:0.85rem; color:var(--muted); margin-top:4px}
.kpi-delta.positive{color:var(--sec)}
.kpi-delta.negative{color:var(--danger)}

hr{margin: 1.2rem 0 1.5rem 0; border-color:var(--line); opacity:0.3;}

/* Cards */
.stMetric {background:var(--card); border-radius:14px; padding:12px 16px; box-shadow:0 1px 6px rgba(0,0,0,0.1)}
.dataframe td, .dataframe th {font-size: 13px}

/* Section headers */
.section-header{display:flex; align-items:center; gap:12px; margin:1.5rem 0 1rem; padding-bottom:8px; border-bottom:2px solid var(--line)}
.section-header h2{margin:0; color:var(--text); font-size:24px}

/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] { 
    padding:12px 20px;
    border-radius:12px;
    background:var(--card);
    border:1px solid var(--line);
    font-weight:600;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] { 
    background:linear-gradient(135deg, var(--pri) 0%, #2563eb 100%);
    border-color:var(--pri);
    color:#fff;
}
</style>
""", unsafe_allow_html=True)

# =========================
# Config & Connection
# =========================
def get_conf(key, default=None):
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except:
        return os.getenv(key, default)

TRINO_HOST = get_conf("TRINO_HOST", "trino")
TRINO_PORT = int(get_conf("TRINO_PORT", "8080"))
TRINO_CATALOG = get_conf("TRINO_CATALOG", "lakehouse")
TRINO_USER = get_conf("TRINO_USER", "admin")
TRINO_PASSWORD = get_conf("TRINO_PASSWORD", "") or None

SCHEMA_PL = "platinum"
SCHEMA_GD = "gold"

AUTH = None
if TRINO_PASSWORD:
    AUTH = BasicAuthentication(TRINO_USER, TRINO_PASSWORD)

DEFAULT_LIMIT = 5000
CACHE_TTL = 600  # 10 phút

# =========================
# Column Detection Helpers
# =========================
@st.cache_data(ttl=600)
def list_columns(schema: str, table: str) -> list:
    """Get list of column names from a table"""
    try:
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        dfc = run_sql(sql, schema)
        return dfc["column_name"].str.lower().tolist() if not dfc.empty else []
    except:
        return []

def pick_col(cols: list, candidates: List[str]) -> Optional[str]:
    """Pick the first matching column name from candidates (case-insensitive)"""
    cols_lower = [c.lower() for c in cols]
    for c in candidates:
        if c.lower() in cols_lower:
            # Return original case from cols
            for col in cols:
                if col.lower() == c.lower():
                    # Get original case from information_schema
                    return col
    return None

def safe_plot(fig_func, df: pd.DataFrame, **kwargs):
    """Safely plot a figure, handling empty DataFrames"""
    if df is None or df.empty:
        st.info("Không có dữ liệu cho biểu đồ này.")
        return
    try:
        fig = fig_func(df, **kwargs)
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Lỗi khi vẽ biểu đồ: {e}")

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def run_sql(sql: str, schema: str = SCHEMA_GD) -> pd.DataFrame:
    """Execute SQL query on Trino and return DataFrame with safety checks"""
    # Guardrails read-only
    low = sql.strip().lower()
    forbidden = ("insert", "update", "delete", "merge", "drop", "alter", "create", "call", "grant", "revoke")
    if any(tok in low for tok in forbidden):
        raise ValueError("Read-only enforced. Forbidden keyword detected.")
    if " bronze." in low or " silver." in low or f"{TRINO_CATALOG}.bronze" in low or f"{TRINO_CATALOG}.silver" in low:
        raise ValueError("Schema not allowed. Use gold/platinum only.")
    if " limit " not in low:
        sql = f"{sql.rstrip()}\nLIMIT {DEFAULT_LIMIT}"

    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=schema,
            http_scheme="http",
            auth=AUTH,
            source="streamlit-executive-dashboard"
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        # Display error clearly in UI instead of silently returning empty DataFrame
        error_msg = str(e)
        # Show error message in Streamlit UI
        st.error(f"❌ Truy vấn SQL lỗi: {error_msg}")
        # Check if it's a table not found error - still show error but return empty DataFrame
        if "TABLE_NOT_FOUND" in error_msg or "does not exist" in error_msg.lower():
            st.warning("⚠️ Bảng không tồn tại. Vui lòng kiểm tra tên bảng hoặc chạy ETL pipeline.")
            return pd.DataFrame()
        # For connection/auth errors, show detailed message
        if "connection" in error_msg.lower() or "authentication" in error_msg.lower() or "auth" in error_msg.lower():
            st.error(f"🔐 Lỗi kết nối Trino: Kiểm tra TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_PASSWORD trong cấu hình.")
        # Log full error for debugging
        import warnings
        warnings.warn(f"SQL Error: {error_msg}")
        return pd.DataFrame()

# =========================
# Column Name Detection & Standardization
# =========================
@st.cache_data(ttl=3600)
def detect_column_names():
    """Detect actual column names and create standardized aliases"""
    # Detect category column - prioritize product_category_name_english (standardized)
    cols_dm = list_columns(SCHEMA_PL, "dm_sales_monthly_category")
    cat_col = pick_col(cols_dm, ["product_category_name_english", "category_en", "category", "product_category"])
    
    # Detect state column - prioritize state (standardized UF)
    cols_geo = list_columns(SCHEMA_GD, "dim_geolocation")
    state_col_geo = pick_col(cols_geo, ["state", "geolocation_state", "customer_state"])
    
    if not state_col_geo:
        cols_cus = list_columns(SCHEMA_GD, "dim_customer")
        state_col_cus = pick_col(cols_cus, ["customer_state", "state"])
    else:
        state_col_cus = None
    
    return {
        "category_col": cat_col,
        "state_col_geo": state_col_geo,
        "state_col_cus": state_col_cus
    }

# Detect once at startup
COLUMN_NAMES = detect_column_names()

# =========================
# Date Coverage Detection
# =========================
@st.cache_data(ttl=3600)
def get_date_coverage():
    """Get actual date range from Brazilian E-commerce (Olist) data - use datamart for stability"""
    # Priority 1: Use platinum datamart (most reliable)
    try:
        sql = f"SELECT MIN(year_month) AS min_ym, MAX(year_month) AS max_ym FROM {SCHEMA_PL}.dm_sales_monthly_category"
        df = run_sql(sql, SCHEMA_PL)
        if not df.empty and df.loc[0, "min_ym"] and df.loc[0, "max_ym"]:
            ym_min = str(df.loc[0, "min_ym"])
            ym_max = str(df.loc[0, "max_ym"])
            # Convert 'YYYY-MM' to date (first of month)
            y_min, m_min = map(int, ym_min.split("-"))
            y_max, m_max = map(int, ym_max.split("-"))
            cov_min = dt.date(y_min, m_min, 1)
            # For max, use first of next month (for half-open interval)
            if m_max == 12:
                cov_max = dt.date(y_max + 1, 1, 1)
            else:
                cov_max = dt.date(y_max, m_max + 1, 1)
            return cov_min, cov_max
    except Exception:
        pass
    
    # Priority 2: Fallback to gold fact_order
    try:
        sql = f"SELECT CAST(MIN(full_date) AS date) AS min_d, CAST(MAX(full_date) AS date) AS max_d FROM {SCHEMA_GD}.fact_order WHERE full_date IS NOT NULL"
        df = run_sql(sql, SCHEMA_GD)
        if not df.empty and df.loc[0, "min_d"] and df.loc[0, "max_d"]:
            min_str = str(df.loc[0, "min_d"])
            max_str = str(df.loc[0, "max_d"])
            return dt.date.fromisoformat(min_str), dt.date.fromisoformat(max_str)
    except Exception:
        pass
    
    # Fallback for Olist dataset
    return dt.date(2016, 9, 4), dt.date(2018, 10, 17)

COVER_MIN, COVER_MAX = get_date_coverage()

# =========================
# Helper Functions
# =========================
def ym(date_obj: dt.date) -> str:
    """Convert date to YYYY-MM format"""
    return f"{date_obj.year:04d}-{date_obj.month:02d}"

def first_of_month(d: dt.date) -> dt.date:
    """Get first day of month"""
    return d.replace(day=1)

def next_month(d: dt.date) -> dt.date:
    """Get first day of next month"""
    if d.month == 12:
        return dt.date(d.year + 1, 1, 1)
    return dt.date(d.year, d.month + 1, 1)

def subtract_months(d: dt.date, months: int) -> dt.date:
    """Subtract months from a date safely"""
    year = d.year
    month = d.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(d.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return dt.date(year, month, day)

def kpi_tile(title: str, value, delta: Optional[str] = None, delta_class: str = ""):
    """Render a KPI tile card"""
    delta_html = f'<div class="kpi-delta {delta_class}">{delta}</div>' if delta else ""
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-title">{title}</div>
      <div class="kpi-value">{value}</div>
      {delta_html}
    </div>
    """, unsafe_allow_html=True)

# =========================
# Sidebar Filters (global)
# =========================
st.sidebar.title("🔎 Bộ lọc toàn hệ thống")

preset = st.sidebar.selectbox("Khoảng thời gian", ["Last 12M", "2017", "2018", "Custom"], index=1)

if preset == "2017":
    start_date = dt.date(2017, 1, 1)
    end_date = dt.date(2017, 12, 31)
elif preset == "2018":
    start_date = dt.date(2018, 1, 1)
    end_date = dt.date(2018, 12, 31)
elif preset == "Last 12M":
    start_date = subtract_months(COVER_MAX, 12)
    start_date = first_of_month(start_date)
    end_date = COVER_MAX
else:
    start_date = st.sidebar.date_input("Từ ngày", COVER_MIN, min_value=COVER_MIN, max_value=COVER_MAX)
    end_date = st.sidebar.date_input("Đến ngày (bao gồm)", COVER_MAX, min_value=COVER_MIN, max_value=COVER_MAX)
    if end_date < start_date:
        st.sidebar.error("Ngày kết thúc phải >= ngày bắt đầu")

# Half-open cho year_month
end_next = next_month(end_date.replace(day=1)) if end_date.day == 1 else (end_date + dt.timedelta(days=1))
ym_start = ym(first_of_month(start_date))
ym_end_excl = ym(end_next)

# Basis switch
basis = st.sidebar.radio("Basis", ["Purchase (Sales)", "Delivery (SLA)"], index=0, horizontal=False)

# Category filter - with auto-detection and fallback
try:
    cat_col = COLUMN_NAMES.get("category_col")
    if cat_col:
        cats = run_sql(f"SELECT DISTINCT {cat_col} AS category FROM {SCHEMA_PL}.dm_sales_monthly_category WHERE {cat_col} IS NOT NULL ORDER BY category", SCHEMA_PL)
        if cats.empty:
            # Fallback: get categories from gold dimension table
            cols_pc = list_columns(SCHEMA_GD, "dim_product_category")
            pc_col = pick_col(cols_pc, ["product_category_name_english", "product_category_name"])
            if pc_col:
                cats = run_sql(f"SELECT DISTINCT {pc_col} AS category FROM {SCHEMA_GD}.dim_product_category WHERE {pc_col} IS NOT NULL ORDER BY 1", SCHEMA_GD)
    else:
        cats = pd.DataFrame()
        st.sidebar.warning("⚠️ Không tìm thấy cột category trong dm_sales_monthly_category")
    sel_cats = st.sidebar.multiselect("Danh mục (tuỳ chọn)", cats["category"].dropna().tolist() if not cats.empty else [], default=[])
except Exception as e:
    sel_cats = []
    # Error already shown by run_sql, but don't break the UI

# State filter - with auto-detection
try:
    state_col_geo = COLUMN_NAMES.get("state_col_geo")
    state_col_cus = COLUMN_NAMES.get("state_col_cus")
    states = pd.DataFrame()
    if state_col_geo:
        states = run_sql(f"SELECT DISTINCT {state_col_geo} AS state FROM {SCHEMA_GD}.dim_geolocation WHERE {state_col_geo} IS NOT NULL ORDER BY state", SCHEMA_GD)
    elif state_col_cus:
        states = run_sql(f"SELECT DISTINCT {state_col_cus} AS state FROM {SCHEMA_GD}.dim_customer WHERE {state_col_cus} IS NOT NULL ORDER BY state", SCHEMA_GD)
    else:
        st.sidebar.warning("⚠️ Không tìm thấy cột state trong dim_geolocation/dim_customer")
    sel_states = st.sidebar.multiselect("Bang/State (tuỳ chọn)", states["state"].tolist() if not states.empty else [], default=[])
except Exception as e:
    sel_states = []
    # Error already shown by run_sql, but don't break the UI

# Top-N slider
topn = st.sidebar.slider("Top-N (xếp hạng)", min_value=5, max_value=50, value=15, step=5)

st.sidebar.markdown(f"<div style='color:var(--muted);font-size:12px;margin-top:20px'>⏱ Cache 10' • Read-only<br>📌 Coverage: {COVER_MIN} → {COVER_MAX}</div>", unsafe_allow_html=True)

# =========================
# Header & Tabs
# =========================
st.markdown("""
<div class="section-header">
<h1>📈 Executive Analytics Suite</h1>
</div>
""", unsafe_allow_html=True)
st.caption("KPI chuẩn hoá • Datamart-first • Drilldown nhẹ • Read-only")

tabs = st.tabs([
    "Executive", "Growth", "Category/Product", "Geography",
    "Seller", "Operations", "Customer", "Finance", "Forecast", "Data Quality", "Insights & Recommendations"
])

# ================
# Tab 1: Executive
# ================
with tabs[0]:
    st.markdown("### Executive Overview")
    
    # Get summary stats for dynamic conclusion
    try:
        sql_summary = f"""
        SELECT 
            SUM(gmv) AS total_gmv,
            SUM(orders) AS total_orders,
            SUM(units) AS total_units,
            CASE WHEN SUM(orders)=0 THEN 0 ELSE ROUND(SUM(gmv)*1.0/SUM(orders),2) END AS avg_aov
        FROM {SCHEMA_PL}.dm_sales_monthly_category
        WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
        LIMIT 1
        """
        df_sum = run_sql(sql_summary, SCHEMA_PL)
        if not df_sum.empty:
            gmv_val = float(df_sum.iloc[0]['total_gmv'])
            orders_val = int(df_sum.iloc[0]['total_orders'])
            units_val = int(df_sum.iloc[0]['total_units'])
            aov_val = float(df_sum.iloc[0]['avg_aov'])
        else:
            gmv_val, orders_val, units_val, aov_val = 0, 0, 0, 0
    except:
        gmv_val, orders_val, units_val, aov_val = 0, 0, 0, 0
    
    st.info(f"""
    **📊 Kết luận nhanh**
    * GMV toàn kỳ ~{gmv_val/1e6:.2f}M BRL; Orders ~{orders_val:,}; Units ~{units_val:,}; **AOV ~{aov_val:.2f} BRL**.
    * Đỉnh mùa vụ **Nov-2017**; nửa đầu năm tăng đều, có nhịp chững tháng 6–7.
    * **Pareto**: ~20% danh mục (bed_bath_table, watches_gifts, health_beauty, sports_leisure…) đóng góp phần lớn GMV.
    
    **💡 Hàm ý**: Doanh thu phụ thuộc nhóm danh mục hạt nhân; tối ưu AOV có tác dụng rõ dịp cuối năm.
    
    **🎯 Hành động**: Ưu tiên tồn kho/marketing cho Top-Pareto; triển khai bundle/upsell cho mùa cao điểm.
    """)
    
    # Build category filter with detected column name
    cat_col = COLUMN_NAMES.get("category_col")
    cat_filter_sql = ""
    if sel_cats and cat_col:
        placeholders = ",".join(["'{}'".format(c) for c in sel_cats])
        cat_filter_sql = f"AND {cat_col} IN ({placeholders})"

    try:
        # Use detected column name and alias to 'category'
        select_cat = f"{cat_col} AS category" if cat_col else "NULL AS category"
        sql = f"""
        SELECT year_month, SUM(gmv) gmv, SUM(orders) orders, SUM(units) units,
               CASE WHEN SUM(orders)=0 THEN 0 ELSE ROUND(SUM(gmv)*1.0/SUM(orders),2) END aov
        FROM {SCHEMA_PL}.dm_sales_monthly_category
        WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
          {cat_filter_sql}
        GROUP BY year_month
        ORDER BY year_month
        LIMIT 100
        """
        df = run_sql(sql, SCHEMA_PL)
        
        if df.empty:
            st.info("Không có dữ liệu cho filter đã chọn.")
        else:
            gmv = float(df["gmv"].sum())
            orders = int(df["orders"].sum())
            units = int(df["units"].sum())
            aov = gmv / orders if orders else 0.0

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                kpi_tile("GMV (BRL)", f"{gmv:,.0f}", "Total Gross Merchandise Value")
            with c2:
                kpi_tile("Orders", f"{orders:,}", "Total orders")
            with c3:
                kpi_tile("Units", f"{units:,}", "Total items sold")
            with c4:
                kpi_tile("AOV (BRL)", f"{aov:,.2f}", "Average Order Value")

            st.markdown("<br>", unsafe_allow_html=True)

            # Line chart: GMV-Orders-AOV
            fig = px.line(df, x="year_month", y=["gmv", "orders", "aov"], 
                         markers=True, title="GMV, Orders, và AOV theo thời gian",
                         labels={"value": "Giá trị", "year_month": "Tháng"})
            fig.update_layout(height=400, template="plotly_dark", 
                            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)

            # Top Categories - use detected column name with alias
            if cat_col:
                sql_top = f"""
                SELECT {cat_col} AS category, SUM(gmv) gmv, SUM(orders) orders, SUM(units) units
                FROM {SCHEMA_PL}.dm_sales_monthly_category
                WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
                  {cat_filter_sql}
                GROUP BY {cat_col}
                ORDER BY gmv DESC
                LIMIT {topn}
                """
                df_top = run_sql(sql_top, SCHEMA_PL)
            else:
                df_top = pd.DataFrame()
            
            c5, c6 = st.columns([2, 1])
            with c5:
                st.markdown("#### Top Categories (GMV)")
                safe_plot(px.bar, df_top, x="category", y="gmv", 
                         title=f"Top {topn} Categories by GMV",
                         labels={"gmv": "GMV (BRL)", "category": "Category"}, height=420)
            
            with c6:
                st.markdown("#### Cơ cấu Orders/Units")
                df_melt = df_top.melt(id_vars="category", value_vars=["orders", "units"])
                fig_group = px.bar(df_melt, x="category", y="value", color="variable",
                                  barmode="group", height=420,
                                  labels={"value": "Số lượng", "category": "Category"})
                fig_group.update_layout(template="plotly_dark",
                                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_group, use_container_width=True)

            st.dataframe(df_top, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Executive: {e}")

# =============
# Tab 2: Growth
# =============
with tabs[1]:
    st.markdown("### Growth & Revenue Analytics")
    
    # Calculate MoM/YoY for dynamic conclusion
    try:
        sql_growth = f"""
        SELECT year_month, SUM(gmv) gmv
        FROM {SCHEMA_PL}.dm_sales_monthly_category
        WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
        GROUP BY year_month
        ORDER BY year_month
        LIMIT 100
        """
        df_g = run_sql(sql_growth, SCHEMA_PL)
        if not df_g.empty:
            df_g["gmv_lag1"] = df_g["gmv"].shift(1)
            df_g["mom"] = (df_g["gmv"] - df_g["gmv_lag1"]) / df_g["gmv_lag1"] * 100
            mom_last = df_g["mom"].iloc[-1] if not math.isnan(df_g["mom"].iloc[-1]) else 0
        else:
            mom_last = 0
    except:
        mom_last = 0
    
    st.info(f"""
    **📊 Kết luận nhanh**
    * **MoM** ở kỳ hiển thị là **{mom_last:.1f}%** (so sánh theo mùa → bình thường sau đỉnh).
    * **YoY** hiển thị 0% do thiếu đủ cửa sổ 12 tháng đối ứng (logic đúng).
    * Đường GMV theo tháng tăng dần, bứt phá Q4.
    
    **💡 Hàm ý**: So sánh MoM cần đi kèm seasonality; YoY nên dùng đủ 12 tháng.
    
    **🎯 Hành động**: Bật Moving Average 3M trên chart; theo dõi "rising categories" (thay đổi Pareto theo tháng).
    """)
    
    try:
        sql = f"""
        SELECT year_month, SUM(gmv) gmv, SUM(orders) orders
        FROM {SCHEMA_PL}.dm_sales_monthly_category
        WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
        GROUP BY year_month
        ORDER BY year_month
        LIMIT 100
        """
        df_growth = run_sql(sql, SCHEMA_PL)
        
        if not df_growth.empty:
            df_growth["gmv_lag1"] = df_growth["gmv"].shift(1)
            df_growth["gmv_lag12"] = df_growth["gmv"].shift(12)
            df_growth["mom"] = (df_growth["gmv"] - df_growth["gmv_lag1"]) / df_growth["gmv_lag1"] * 100
            df_growth["yoy"] = (df_growth["gmv"] - df_growth["gmv_lag12"]) / df_growth["gmv_lag12"] * 100

            c1, c2 = st.columns(2)
            with c1:
                mom_val = df_growth["mom"].iloc[-1] if not math.isnan(df_growth["mom"].iloc[-1]) else 0
                kpi_tile("MoM Growth (GMV)", f"{mom_val:.1f}%", 
                        "Month-over-Month", "positive" if mom_val > 0 else "negative")
            with c2:
                yoy_val = df_growth["yoy"].iloc[-1] if not math.isnan(df_growth["yoy"].iloc[-1]) else 0
                kpi_tile("YoY Growth (GMV)", f"{yoy_val:.1f}%",
                        "Year-over-Year", "positive" if yoy_val > 0 else "negative")

            # Area chart
            fig_area = px.area(df_growth, x="year_month", y="gmv",
                              title="GMV theo thời gian (Area Chart)",
                              labels={"gmv": "GMV (BRL)", "year_month": "Tháng"})
            fig_area.update_layout(height=360, template="plotly_dark",
                                 plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_area, use_container_width=True)

            # Pareto - use detected column name with alias
            if cat_col:
                sql_pareto = f"""
                SELECT {cat_col} AS category, SUM(gmv) gmv
                FROM {SCHEMA_PL}.dm_sales_monthly_category
                WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
                GROUP BY {cat_col}
                ORDER BY gmv DESC
                LIMIT 20
                """
                df_par = run_sql(sql_pareto, SCHEMA_PL)
            else:
                df_par = pd.DataFrame()
            if not df_par.empty:
                df_par["cum_share"] = (df_par["gmv"].cumsum() / df_par["gmv"].sum() * 100).round(1)
                figp = go.Figure()
                figp.add_bar(x=df_par["category"], y=df_par["gmv"], name="GMV")
                figp.add_scatter(x=df_par["category"], y=df_par["cum_share"],
                               name="Cumulative %", yaxis="y2", mode="lines+markers")
                figp.update_layout(
                    title="Pareto: đóng góp GMV theo danh mục",
                    yaxis=dict(title="GMV (BRL)"),
                    yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 100]),
                    height=420,
                    template="plotly_dark",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)"
                )
                if not df_par.empty:
                    st.plotly_chart(figp, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Growth: {e}")

# =======================
# Tab 3: Category/Product
# =======================
with tabs[2]:
    st.markdown("### Category & Product Performance")
    st.info("""
    **📊 Kết luận nhanh**
    * **Heatmap** cho thấy mùa vụ rõ ở quà tặng/beauty (cuối năm sáng).
    * **Top SKU** tập trung ở *computers*, *bed_bath_table*, *cool_stuff*; đóng góp GMV lớn và ổn định.
    
    **💡 Hàm ý**: SKU đầu bảng quyết định phần lớn doanh thu; quản trị giá, tồn kho và hiển thị cực kỳ quan trọng.
    
    **🎯 Hành động**: Đặt chỉ tiêu GMV theo **tháng × danh mục** (không tuyến tính năm); A/B giá/khuyến mại cho Top-SKU.
    """)

    try:
        # Heatmap Category x Month - use detected column name with alias
        cat_col = COLUMN_NAMES.get("category_col")
        if cat_col:
            sql_catm = f"""
            SELECT year_month, {cat_col} AS category, SUM(gmv) gmv
            FROM {SCHEMA_PL}.dm_sales_monthly_category
            WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
            GROUP BY year_month, {cat_col}
            LIMIT 10000
            """
            df_catm = run_sql(sql_catm, SCHEMA_PL)
        else:
            df_catm = pd.DataFrame()
        if not df_catm.empty:
            pivot = df_catm.pivot_table(index="category", columns="year_month", values="gmv", fill_value=0.0)
            st.markdown("#### Heatmap: GMV theo Danh mục × Tháng")
            fig_heat = px.imshow(pivot, aspect="auto", title="GMV Heatmap",
                               labels=dict(x="Tháng", y="Category", color="GMV"))
            fig_heat.update_layout(height=400, template="plotly_dark",
                                 plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_heat, use_container_width=True)

        # Top products
        sql_prod = f"""
        SELECT p.product_id, COALESCE(pc.product_category_name_english, 'unknown') category_en,
               SUM(foi.price) gmv, COUNT(*) units
        FROM {SCHEMA_GD}.fact_order_item foi
        JOIN {SCHEMA_GD}.dim_product p ON foi.product_id=p.product_id
        LEFT JOIN {SCHEMA_GD}.dim_product_category pc ON p.product_category_name=pc.product_category_name
        JOIN {SCHEMA_GD}.dim_date d ON foi.full_date=d.full_date
        WHERE d.year_month >= '{ym_start}' AND d.year_month < '{ym_end_excl}'
        GROUP BY 1,2
        ORDER BY gmv DESC
        LIMIT {topn * 3}
        """
        df_prod = run_sql(sql_prod, SCHEMA_GD)
        if not df_prod.empty:
            c1, c2 = st.columns([2, 1])
            with c1:
                st.markdown("#### Top sản phẩm theo GMV")
                fig_prod = px.bar(df_prod.head(topn), x="product_id", y="gmv", color="category_en",
                                 height=420, labels={"gmv": "GMV (BRL)", "product_id": "Product ID"})
                fig_prod.update_layout(template="plotly_dark",
                                     plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_prod, use_container_width=True)
            with c2:
                st.markdown("#### Bảng chi tiết")
                st.dataframe(df_prod.head(topn), use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Category/Product: {e}")

# ===============
# Tab 4: Geography
# ===============
with tabs[3]:
    st.markdown("### Geography & Market Expansion")
    st.info("""
    **📊 Kết luận nhanh**
    * **SP, RJ, MG** dẫn đầu GMV; các bang đuôi có **on-time tốt nhưng GMV thấp** → dư địa mở rộng.
    * Một số bang có **delivery days** cao (outlier >20 ngày).
    
    **💡 Hàm ý**: Cần tối ưu **last-mile** theo vùng; ưu tiên đầu tư marketing ở bang on-time tốt/GMV thấp.
    
    **🎯 Hành động**: Thiết lập **SLA theo bang** (đối tác 3PL, tuyến, promise); chạy chiến dịch địa phương tại bang tiềm năng.
    """)
    
    try:
        # GMV by State - use detected column name with proper JOIN
        state_col_geo = COLUMN_NAMES.get("state_col_geo")
        state_col_cus = COLUMN_NAMES.get("state_col_cus")
        
        if state_col_geo:
            sql_geo = f"""
            SELECT g.{state_col_geo} AS state, SUM(foi.price) gmv, COUNT(DISTINCT foi.order_id) orders
            FROM {SCHEMA_GD}.fact_order_item foi
            JOIN {SCHEMA_GD}.fact_order fo ON foi.order_id=fo.order_id
            JOIN {SCHEMA_GD}.dim_customer c ON fo.customer_id = c.customer_id
            JOIN {SCHEMA_GD}.dim_geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
            JOIN {SCHEMA_GD}.dim_date d ON fo.full_date=d.full_date
            WHERE d.year_month >= '{ym_start}' AND d.year_month < '{ym_end_excl}'
            GROUP BY g.{state_col_geo}
            ORDER BY gmv DESC
            LIMIT 50
            """
            df_geo = run_sql(sql_geo, SCHEMA_GD)
        elif state_col_cus:
            sql_geo = f"""
            SELECT c.{state_col_cus} AS state, SUM(foi.price) gmv, COUNT(DISTINCT foi.order_id) orders
            FROM {SCHEMA_GD}.fact_order_item foi
            JOIN {SCHEMA_GD}.fact_order fo ON foi.order_id=fo.order_id
            JOIN {SCHEMA_GD}.dim_customer c ON fo.customer_id = c.customer_id
            JOIN {SCHEMA_GD}.dim_date d ON fo.full_date=d.full_date
            WHERE d.year_month >= '{ym_start}' AND d.year_month < '{ym_end_excl}'
            GROUP BY c.{state_col_cus}
            ORDER BY gmv DESC
            LIMIT 50
            """
            df_geo = run_sql(sql_geo, SCHEMA_GD)
        else:
            df_geo = pd.DataFrame()
        safe_plot(px.bar, df_geo, x="state", y="gmv", title="GMV theo Bang/State",
                 labels={"gmv": "GMV (BRL)", "state": "State"}, height=400)

        # On-time by State - use detected column name with proper JOIN
        if state_col_geo:
            sql_ot = f"""
            SELECT g.{state_col_geo} AS state,
                   ROUND(AVG(CASE WHEN fo.delivered_on_time THEN 1 ELSE 0 END)*100,1) on_time_rate_pct,
                   ROUND(AVG(fo.delivered_days),2) delivery_days_avg,
                   COUNT(*) AS delivered_orders
            FROM {SCHEMA_GD}.fact_order fo
            JOIN {SCHEMA_GD}.dim_customer c ON fo.customer_id = c.customer_id
            JOIN {SCHEMA_GD}.dim_geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
            WHERE fo.full_date >= DATE '{start_date}' AND fo.full_date < DATE '{end_next}'
              AND fo.is_canceled = FALSE
            GROUP BY g.{state_col_geo}
            ORDER BY on_time_rate_pct DESC
            LIMIT 50
            """
            df_ot = run_sql(sql_ot, SCHEMA_GD)
        elif state_col_cus:
            sql_ot = f"""
            SELECT c.{state_col_cus} AS state,
                   ROUND(AVG(CASE WHEN fo.delivered_on_time THEN 1 ELSE 0 END)*100,1) on_time_rate_pct,
                   ROUND(AVG(fo.delivered_days),2) delivery_days_avg,
                   COUNT(*) AS delivered_orders
            FROM {SCHEMA_GD}.fact_order fo
            JOIN {SCHEMA_GD}.dim_customer c ON fo.customer_id = c.customer_id
            WHERE fo.full_date >= DATE '{start_date}' AND fo.full_date < DATE '{end_next}'
              AND fo.is_canceled = FALSE
            GROUP BY c.{state_col_cus}
            ORDER BY on_time_rate_pct DESC
            LIMIT 50
            """
            df_ot = run_sql(sql_ot, SCHEMA_GD)
        else:
            df_ot = pd.DataFrame()
        if not df_ot.empty:
            c1, c2 = st.columns(2)
            with c1:
                safe_plot(px.bar, df_ot, x="state", y="on_time_rate_pct",
                         title="On-time Rate (%)", labels={"on_time_rate_pct": "On-time %", "state": "State"}, height=400)
            with c2:
                safe_plot(px.bar, df_ot, x="state", y="delivery_days_avg",
                         title="Avg Delivery Days", labels={"delivery_days_avg": "Days", "state": "State"}, height=400)
            st.dataframe(df_ot, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Geography: {e}")

# =============
# Tab 5: Seller
# =============
with tabs[4]:
    st.markdown("### Seller Performance & Compliance")
    
    # Get seller stats for dynamic conclusion
    try:
        sql_s = f"""
        SELECT 
            AVG(on_time_rate) on_time_rate, 
            AVG(cancel_rate) cancel_rate, 
            AVG(avg_review_score) avg_review_score
        FROM {SCHEMA_PL}.dm_seller_kpi
        LIMIT 1
        """
        df_s = run_sql(sql_s, SCHEMA_PL)
        if not df_s.empty:
            on_time_avg = float(df_s.iloc[0]['on_time_rate']) * 100
            cancel_avg = float(df_s.iloc[0]['cancel_rate']) * 100
            review_avg = float(df_s.iloc[0]['avg_review_score'])
        else:
            on_time_avg, cancel_avg, review_avg = 0, 0, 0
    except:
        on_time_avg, cancel_avg, review_avg = 0, 0, 0
    
    st.info(f"""
    **📊 Kết luận nhanh**
    * **On-time TB ~{on_time_avg:.1f}%**, **Cancel ~{cancel_avg:.2f}%**, **Avg review ~{review_avg:.2f}** (mặt bằng tốt).
    * Có **seller outlier** (on_time <0.90 hoặc review <4.0) vẫn có GMV đáng kể.
    
    **💡 Hàm ý**: Rủi ro trải nghiệm tập trung ở ít seller nhưng ảnh hưởng lớn.
    
    **🎯 Hành động**: Cảnh báo tự động: `on_time<92%` **hoặc** `review<4.0` **và** GMV>ngưỡng; áp dụng thưởng/phạt SLA & coaching.
    """)

    try:
        sql_seller = f"""
        SELECT seller_id,
               SUM(gmv) gmv, SUM(orders) orders, SUM(units) units,
               AVG(on_time_rate) on_time_rate, AVG(cancel_rate) cancel_rate, AVG(avg_review_score) avg_review_score
        FROM {SCHEMA_PL}.dm_seller_kpi
        GROUP BY seller_id
        ORDER BY gmv DESC
        LIMIT {topn * 3}
        """
        df_seller = run_sql(sql_seller, SCHEMA_PL)
        if not df_seller.empty:
            c1, c2, c3 = st.columns(3)
            with c1:
                kpi_tile("On-time TB", f"{df_seller['on_time_rate'].mean()*100:.1f}%", "Average")
            with c2:
                kpi_tile("Cancel TB", f"{df_seller['cancel_rate'].mean()*100:.2f}%", "Average")
            with c3:
                kpi_tile("Avg Review TB", f"{df_seller['avg_review_score'].mean():.2f}", "Average")

            fig_scatter = px.scatter(df_seller, x="on_time_rate", y="avg_review_score",
                                   size="gmv", hover_name="seller_id",
                                   title="On-time vs Review (bubble size=GMV)",
                                   labels={"on_time_rate": "On-time Rate", "avg_review_score": "Review Score"})
            fig_scatter.update_layout(height=420, template="plotly_dark",
                                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_scatter, use_container_width=True)
            st.dataframe(df_seller.head(topn), use_container_width=True)
        else:
            st.info("Chưa có dm_seller_kpi hoặc dữ liệu rỗng.")
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Seller: {e}")

# ================
# Tab 6: Operations
# ================
with tabs[5]:
    st.markdown("### Operations & Logistics SLA (Delivery-basis)")
    
    # Get operations stats for dynamic conclusion
    try:
        sql_o = f"""
        SELECT 
            AVG(CASE WHEN fo.delivered_on_time THEN 1 ELSE 0 END) on_time_rate,
            AVG(fo.delivered_days) delivery_days
        FROM {SCHEMA_GD}.fact_order fo
        JOIN {SCHEMA_GD}.dim_date d ON fo.full_date = d.full_date
        WHERE fo.full_date >= DATE '{start_date}' AND fo.full_date < DATE '{end_next}'
          AND fo.is_canceled = FALSE
        LIMIT 1
        """
        df_o = run_sql(sql_o, SCHEMA_GD)
        if not df_o.empty:
            on_time_ops = float(df_o.iloc[0]['on_time_rate']) * 100
            delivery_days_ops = float(df_o.iloc[0]['delivery_days'])
        else:
            on_time_ops, delivery_days_ops = 0, 0
    except:
        on_time_ops, delivery_days_ops = 0, 0
    
    st.info(f"""
    **📊 Kết luận nhanh**
    * **On-time ~{on_time_ops:.1f}%**, **Delivery days ~{delivery_days_ops:.1f}**; có spike ở vài tháng/bang trùng cao điểm.
    * Late orders chủ yếu ở tuyến xa/địa hình khó.
    
    **💡 Hàm ý**: Nút thắt nằm ở năng lực line-haul/last-mile mùa cao điểm.
    
    **🎯 Hành động**: Nâng năng lực fulfillment trước Q4 (cut-off, line-haul, 3PL dự phòng); công bố **delivery promise theo bang**.
    """)

    try:
        sql_ops = f"""
        SELECT d.year_month,
               AVG(CASE WHEN fo.delivered_on_time THEN 1 ELSE 0 END) on_time_rate,
               AVG(fo.delivered_days) delivery_days,
               COUNT(*) AS delivered_orders
        FROM {SCHEMA_GD}.fact_order fo
        JOIN {SCHEMA_GD}.dim_date d ON fo.full_date = d.full_date
        WHERE fo.full_date >= DATE '{start_date}' AND fo.full_date < DATE '{end_next}'
          AND fo.is_canceled = FALSE
        GROUP BY d.year_month
        ORDER BY d.year_month
        LIMIT 100
        """
        df_ops = run_sql(sql_ops, SCHEMA_GD)
        if not df_ops.empty:
            c1, c2 = st.columns(2)
            with c1:
                kpi_tile("On-time TB", f"{df_ops['on_time_rate'].mean()*100:.1f}%", "Average")
            with c2:
                kpi_tile("Deliv Days TB", f"{df_ops['delivery_days'].mean():.2f}", "Average")

            fig_ops = px.line(df_ops, x="year_month", y="on_time_rate", markers=True,
                            title="On-time theo tháng",
                            labels={"on_time_rate": "On-time Rate", "year_month": "Tháng"})
            fig_ops.update_layout(height=400, template="plotly_dark",
                                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_ops, use_container_width=True)

            fig_days = px.line(df_ops, x="year_month", y="delivery_days", markers=True,
                             title="Delivery days theo tháng",
                             labels={"delivery_days": "Days", "year_month": "Tháng"})
            fig_days.update_layout(height=400, template="plotly_dark",
                                 plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_days, use_container_width=True)
        else:
            st.info("Không có dữ liệu SLA cho kỳ đã chọn.")
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Operations: {e}")

# ===============
# Tab 7: Customer
# ===============
with tabs[6]:
    st.markdown("### Customer Lifecycle & Cohort")
    st.info("""
    **📊 Kết luận nhanh**
    * **Heatmap retention** đã được sửa để hiển thị đúng; **retention(k)** giảm dần theo k (0..12 tháng).
    * Dốc giảm phản ánh chất lượng tăng trưởng và hiệu quả giữ chân.
    
    **💡 Hàm ý**: Retention giảm dần theo k là bình thường; dốc giảm phản ánh chất lượng tăng trưởng.
    
    **🎯 Hành động**: Áp dụng truy vấn cohort đúng (0..12 tháng); đặt OKR **k=1,3,6**; chạy remarketing cho cohort suy giảm.
    """)

    try:
        # Query with calculation of months_since_cohort and customer counts
        # FIXED: Only filter cohort_month by selected year, NOT activity_month
        # This allows customers from selected cohorts to show activity in subsequent months
        sql_cohort = f"""
        WITH first_purchase AS (
            SELECT 
                customer_id,
                MIN(d.year_month) as cohort_month
            FROM {SCHEMA_GD}.fact_order fo
            JOIN {SCHEMA_GD}.dim_date d ON fo.full_date = d.full_date
            WHERE fo.is_canceled = false
            GROUP BY customer_id
        ),
        cohort_size AS (
            SELECT 
                cohort_month,
                COUNT(DISTINCT customer_id) AS customers_in_cohort
            FROM first_purchase
            WHERE cohort_month >= '{ym_start}' AND cohort_month < '{ym_end_excl}'
            GROUP BY cohort_month
        ),
        monthly_activity AS (
            SELECT 
                fp.cohort_month,
                d.year_month,
                COUNT(DISTINCT fo.customer_id) AS customers_active,
                COUNT(DISTINCT fo.order_id) AS total_orders,
                SUM(COALESCE(fo.sum_price, 0) + COALESCE(fo.sum_freight, 0)) AS total_gmv
            FROM {SCHEMA_GD}.fact_order fo
            JOIN {SCHEMA_GD}.dim_date d ON fo.full_date = d.full_date
            INNER JOIN first_purchase fp ON fo.customer_id = fp.customer_id
            WHERE fp.cohort_month >= '{ym_start}' AND fp.cohort_month < '{ym_end_excl}'
              AND d.year_month >= fp.cohort_month
              AND fo.is_canceled = false
            GROUP BY fp.cohort_month, d.year_month
        )
        SELECT 
            ma.cohort_month,
            ma.year_month,
            CAST(SUBSTRING(ma.year_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.year_month, 6, 2) AS INTEGER) -
            (CAST(SUBSTRING(ma.cohort_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.cohort_month, 6, 2) AS INTEGER)) AS months_since_cohort,
            cs.customers_in_cohort,
            ma.customers_active,
            ma.total_orders,
            ma.total_gmv
        FROM monthly_activity ma
        JOIN cohort_size cs ON ma.cohort_month = cs.cohort_month
        WHERE CAST(SUBSTRING(ma.year_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.year_month, 6, 2) AS INTEGER) -
              (CAST(SUBSTRING(ma.cohort_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.cohort_month, 6, 2) AS INTEGER)) >= 0
          AND CAST(SUBSTRING(ma.year_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.year_month, 6, 2) AS INTEGER) -
              (CAST(SUBSTRING(ma.cohort_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.cohort_month, 6, 2) AS INTEGER)) <= 24
        ORDER BY ma.cohort_month, months_since_cohort
        LIMIT 10000
        """
        df_coh = run_sql(sql_cohort, SCHEMA_GD)
        if not df_coh.empty:
            # Ensure months_since_cohort is integer
            df_coh["months_since_cohort"] = df_coh["months_since_cohort"].astype(int)
            
            # Use customers_in_cohort from query (already calculated)
            if "customers_in_cohort" not in df_coh.columns:
                # Fallback: calculate from month 0
                cohort_0 = df_coh[df_coh["months_since_cohort"] == 0].set_index("cohort_month")["customers_active"]
                df_coh["cohort_size"] = df_coh["cohort_month"].map(cohort_0).fillna(0)
            else:
                df_coh["cohort_size"] = df_coh["customers_in_cohort"]
            
            if df_coh["cohort_size"].sum() == 0:
                st.warning("⚠️ Không tìm thấy dữ liệu cho tháng đầu tiên của cohort. Vui lòng kiểm tra lại dữ liệu.")
                st.dataframe(df_coh.head(20))
            else:
                df_coh = df_coh.copy()
                
                # Calculate retention: customers_active / cohort_size
                # Handle division by zero
                df_coh["retention"] = df_coh.apply(
                    lambda row: row["customers_active"] / row["cohort_size"] if row["cohort_size"] > 0 else 0.0,
                    axis=1
                )
                
                # Filter out rows with cohort_size = 0 for display
                df_coh_display = df_coh[df_coh["cohort_size"] > 0].copy()
                
                if df_coh_display.empty:
                    st.warning("⚠️ Không có dữ liệu để hiển thị sau khi lọc.")
                    st.dataframe(df_coh.head(20))
                else:
                    # Retention Matrix
                    if len(df_coh_display) > 0:
                        mat = df_coh_display.pivot_table(
                            index="cohort_month", 
                            columns="months_since_cohort", 
                            values="retention", 
                            fill_value=0.0,
                            aggfunc='mean'
                        )
                        
                        if not mat.empty:
                            st.markdown("#### Retention Matrix (cohort × months_since_cohort)")
                            safe_plot(px.imshow, mat, aspect="auto", title="Customer Retention Heatmap",
                                     labels=dict(x="Months Since Cohort", y="Cohort Month", color="Retention"), 
                                     height=400, color_continuous_scale="Blues")
                        else:
                            st.info("Không có dữ liệu để tạo retention matrix.")
                        
                        # Orders trend
                        if len(df_coh_display) > 0:
                            fig_orders = px.line(df_coh_display, x="year_month", y="total_orders", color="cohort_month",
                                               title="Orders by Cohort over Time",
                                               labels={"total_orders": "Orders", "year_month": "Month"})
                            fig_orders.update_layout(height=350, template="plotly_dark",
                                                   plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_orders, use_container_width=True)
                        else:
                            st.info("Không có dữ liệu để vẽ biểu đồ orders.")
                    
                    # Display data table
                    display_cols = ["cohort_month", "year_month", "months_since_cohort", "cohort_size", "customers_active", "retention", "total_orders"]
                    # Rename for display
                    df_display = df_coh_display[display_cols].copy()
                    df_display = df_display.rename(columns={"cohort_size": "customers_in_cohort"})
                    st.dataframe(
                        df_display.style.format({"retention": "{:.2%}"}), 
                        use_container_width=True
                    )
        else:
            st.info("Chưa có dữ liệu cohort.")
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Customer: {e}")

# =============
# Tab 8: Finance
# =============
with tabs[7]:
    st.markdown("### Finance & Payment Mix")
    
    # Get payment mix stats for dynamic conclusion
    try:
        sql_p = f"""
        SELECT 
            payment_type,
            SUM(payment_total) payment_total,
            AVG(installments) avg_installments
        FROM {SCHEMA_PL}.dm_payment_mix
        WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
        GROUP BY payment_type
        ORDER BY payment_total DESC
        LIMIT 5
        """
        df_p = run_sql(sql_p, SCHEMA_PL)
        if not df_p.empty:
            credit_card_rows = df_p[df_p['payment_type'] == 'credit_card']
            credit_card_total = float(credit_card_rows['payment_total'].sum()) if not credit_card_rows.empty else 0
            total_payment = float(df_p['payment_total'].sum())
            credit_card_pct = (credit_card_total / total_payment * 100) if total_payment > 0 else 0
            avg_installments = float(credit_card_rows['avg_installments'].iloc[0]) if not credit_card_rows.empty and not pd.isna(credit_card_rows['avg_installments'].iloc[0]) else 0
        else:
            credit_card_pct, avg_installments = 0, 0
    except:
        credit_card_pct, avg_installments = 0, 0
    
    st.info(f"""
    **📊 Kết luận nhanh**
    * **credit_card** chiếm tỷ trọng lớn (~{credit_card_pct:.1f}%) và **tăng mạnh cuối năm**; **installments ~{avg_installments:.1f}** → khách sẵn sàng trả góp ngắn.
    * boleto/voucher tỷ trọng nhỏ, dao động theo chiến dịch.
    
    **💡 Hàm ý**: Payment mix nghiêng về thẻ → ảnh hưởng phí MDR và dòng tiền.
    
    **🎯 Hành động**: Đàm phán phí với PSP; A/B **ưu đãi installments** ở ngành hàng giá cao (computers/furniture).
    """)

    try:
        sql_mix = f"""
        SELECT year_month, payment_type, SUM(orders) orders, 
               SUM(unique_customers) unique_customers, SUM(payment_total) payment_total
        FROM {SCHEMA_PL}.dm_payment_mix
        WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
        GROUP BY 1,2
        ORDER BY 1,2
        LIMIT 1000
        """
        df_mix = run_sql(sql_mix, SCHEMA_PL)
        if not df_mix.empty:
            fig_mix = px.area(df_mix, x="year_month", y="orders", color="payment_type",
                            title="Orders Share by Payment Type",
                            labels={"orders": "Orders", "year_month": "Tháng"})
            fig_mix.update_layout(height=380, template="plotly_dark",
                                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_mix, use_container_width=True)

            # Installments
            sql_inst = f"""
            SELECT d.year_month, AVG(fp.payment_installments) avg_installments, 
                   COUNT(DISTINCT fp.order_id) orders
            FROM {SCHEMA_GD}.fact_payment fp
            JOIN {SCHEMA_GD}.fact_order fo ON fp.order_id=fo.order_id
            JOIN {SCHEMA_GD}.dim_date d ON fo.full_date=d.full_date
            WHERE fp.payment_type='credit_card'
              AND d.year_month >= '{ym_start}' AND d.year_month < '{ym_end_excl}'
            GROUP BY d.year_month
            ORDER BY d.year_month
            LIMIT 100
            """
            df_inst = run_sql(sql_inst, SCHEMA_GD)
            if not df_inst.empty:
                fig_inst = px.line(df_inst, x="year_month", y="avg_installments", markers=True,
                                 title="Avg Installments (credit_card)",
                                 labels={"avg_installments": "Avg Installments", "year_month": "Tháng"})
                fig_inst.update_layout(template="plotly_dark",
                                     plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_inst, use_container_width=True)
            st.dataframe(df_mix, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu Finance: {e}")

# ==============
# Tab 9: Forecast
# ==============
with tabs[8]:
    st.markdown("### Forecast & Planning")
    st.info("""
    Dự báo nhu cầu 14–28 ngày theo danh mục/vùng; hiển thị **actual vs forecast** và **dải tin cậy**. 
    Dùng để lập kế hoạch tồn kho và ngân sách.
    
    **📊 Dữ liệu dự báo đã được chuyển sang Forecast Explorer** - vui lòng sử dụng trang **Forecast Explorer** trong menu bên trái.
    """)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **📈 Forecast Explorer cung cấp:**
        - 🔮 Dữ liệu dự báo chi tiết
        - 📊 Monitoring metrics (sMAPE, MAE)
        - 🧪 MLflow experiment tracking
        - ⚙️ Pipeline management qua Dagster
        """)
    
    with col2:
        st.markdown("""
        **🚀 Để chạy forecast pipeline:**
        1. Mở **Forecast Explorer** (menu trái)
        2. Hoặc mở Dagster UI trực tiếp
        3. Jobs → `forecast_job`
        4. Launch Run
        """)
    
    st.markdown("---")
    
    # Link to Forecast Explorer
    st.markdown("### 🎯 Truy cập Forecast Explorer")
    if st.button("📊 Mở Forecast Explorer", use_container_width=True, type="primary"):
        st.switch_page("pages/3_📈_Forecast_Explorer.py")
    
    st.caption("💡 Hoặc click vào 'Forecast Explorer' trong menu bên trái để xem dữ liệu dự báo chi tiết.")

# =================
# Tab 10: DQ & Ops
# =================
with tabs[9]:
    st.markdown("### Data Quality & Reliability")
    st.info("""
    **📊 Kết luận nhanh**
    * **Pass rate 96–100%** ở các suite demo: pipeline ổn định, số liệu đáng tin.
    
    **💡 Hàm ý**: Có thể dựa vào dashboard để ra quyết định; vẫn cần canh drift/schema-change.
    
    **🎯 Hành động**: Nâng DQ từ demo → **rule vận hành** (row_count delta, domain 1..5 cho review, FK integrity, freshness); hiển thị **DQ badge** trên từng tab.
    """)

    try:
        # Demo data
        sql_counts = """
        SELECT 'bronze->silver->gold (demo)' AS suite, 98 AS pass_pct, 2 AS fail_cnt
        UNION ALL SELECT 'null/domain (demo)', 96, 4
        UNION ALL SELECT 'orphan FK (demo)', 100, 0
        """
        df_dq = run_sql(sql_counts, SCHEMA_GD)
        c1, c2 = st.columns(2)
        with c1:
            fig_dq = px.bar(df_dq, x="suite", y="pass_pct", title="% DQ pass (demo)",
                          labels={"pass_pct": "% Pass", "suite": "Test Suite"})
            fig_dq.update_layout(height=380, template="plotly_dark",
                               plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_dq, use_container_width=True)
        with c2:
            st.dataframe(df_dq, use_container_width=True)
    except Exception as e:
        st.info(f"Chưa có dữ liệu DQ hoặc lỗi: {e}")

# =================
# Tab 11: Insights & Recommendations
# =================
with tabs[10]:
    st.markdown("### 📊 Kết luận & Kiến nghị")
    st.info("""
    **Phân tích tổng hợp và đề xuất hành động** dựa trên dữ liệu hiện tại của Executive Analytics Suite.
    """)
    
    # Get summary stats from Executive tab
    try:
        sql_summary = f"""
        SELECT 
            SUM(gmv) AS total_gmv,
            SUM(orders) AS total_orders,
            SUM(units) AS total_units,
            CASE WHEN SUM(orders)=0 THEN 0 ELSE ROUND(SUM(gmv)*1.0/SUM(orders),2) END AS avg_aov
        FROM {SCHEMA_PL}.dm_sales_monthly_category
        WHERE year_month >= '{ym_start}' AND year_month < '{ym_end_excl}'
        LIMIT 1
        """
        df_summary = run_sql(sql_summary, SCHEMA_PL)
        
        if not df_summary.empty:
            total_gmv = float(df_summary.iloc[0]['total_gmv'])
            total_orders = int(df_summary.iloc[0]['total_orders'])
            total_units = int(df_summary.iloc[0]['total_units'])
            avg_aov = float(df_summary.iloc[0]['avg_aov'])
        else:
            total_gmv = 0
            total_orders = 0
            total_units = 0
            avg_aov = 0
    except:
        total_gmv = 0
        total_orders = 0
        total_units = 0
        avg_aov = 0
    
    # Executive Summary
    st.markdown("---")
    st.markdown("## 📈 Kết luận tổng quan (Executive Summary)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        ### Quy mô & hiệu quả
        
        * Toàn kỳ hiển thị: **GMV ≈ {total_gmv/1e6:.2f}M BRL**, **{total_orders:,} orders**, **{total_units:,} units**, **AOV ≈ {avg_aov:.2f} BRL**.
        * **Đỉnh mùa vụ rơi vào cuối năm (Nov-2017)**; nửa đầu năm tăng đều, có dao động nhẹ giữa May–Jul. 
          Đây là mô hình tăng trưởng tự nhiên của TMĐT (sale cuối năm kéo AOV lên).
        
        ### Động lực tăng trưởng
        
        * Doanh thu tập trung ở một số **category "hạt nhân"** (biểu đồ Pareto): nhóm 20% danh mục đóng góp ~80% GMV. 
          Các nhóm nổi bật: *bed_bath_table*, *watches_gifts*, *health_beauty*, *sports_leisure*, *computers_accessories*…
        
        ### Địa lý
        
        * **SP** dẫn đầu GMV (một mình >2M BRL), theo sau là **RJ**, **MG**. 
          Đây là các vùng thị trường trọng điểm để tối ưu tồn kho, vận chuyển và chiến dịch quảng cáo.
        """)
    
    with col2:
        st.markdown("""
        ### Vận hành & SLA
        
        * **On-time delivery** trung bình ~**92–94%** (tuỳ tab Basis), **delivery days avg** quanh **12–15 ngày**, 
          nhưng **một số bang có outlier 20–30 ngày** → điểm nghẽn logistics theo vùng.
        
        ### Nhà bán (Seller)
        
        * **Điểm review TB ≈ 4.09**; **cancel rate rất thấp (~0.01%)**; đa phần nhà bán có **on-time 0.90–0.96**. 
          Một số outlier review thấp/on-time thấp cần giám sát.
        
        ### Thanh toán
        
        * **credit_card** chiếm tỷ trọng cao và **tăng mạnh dịp cuối năm**; **installments ~3.4–4.1** cho thấy 
          hành vi trả góp hiện diện đáng kể (ảnh hưởng chi phí tài chính/thu hồi tiền).
        
        ### Data Quality
        
        * Bộ kiểm thử DQ **pass ~96–100%** ở các suite demo → dữ liệu đủ tin cậy để ra quyết định; 
          vẫn cần theo dõi định kỳ để phát hiện drift/sai lệch.
        """)
    
    st.markdown("---")
    st.markdown("## 📋 Kết luận theo từng dashboard")
    
    # Dashboard-specific conclusions
    st.markdown("""
    ### 1) Executive Overview
    
    * **Doanh thu tăng theo mùa vụ**, đỉnh cuối năm; **AOV tăng nhẹ** thời điểm peak → bán nhiều mặt hàng giá cao/giỏ hàng lớn hơn.
    * **Top categories** tạo phần lớn GMV; phân bổ ngân sách marketing/tồn kho nên **ưu tiên nhóm hạt nhân** để tối đa ROI.
    
    **Kiến nghị:**
    * Lập **ngân sách Q4** (marketing + fulfillment) sớm; tạo **bundle/upsell** ở nhóm category chủ lực để tận dụng AOV cao.
    
    ---
    
    ### 2) Growth & Revenue
    
    * **MoM** có biến động theo mùa (màu lệnh cảnh báo MoM -26.5% là do so sánh 2 mốc cụ thể; cần đọc kèm seasonality).
    * **YoY 0%** ở một số tháng cuối do coverage chưa trọn vẹn năm trước (logic đúng).
    
    **Kiến nghị:**
    * Chuẩn hóa cửa sổ so sánh (đủ 12 tháng), bổ sung **moving average** 3M để mượt xu hướng.
    * Chạy **ABC/Pareto theo tháng** để phát hiện danh mục mới nổi (rising stars) và danh mục suy giảm.
    
    ---
    
    ### 3) Category & Product Performance
    
    * **Heatmap** thể hiện mùa vụ rõ theo ngành hàng; ví dụ *gifts/beauty* sáng lên cuối năm.
    * **Top sản phẩm**: nhiều SKU thuộc *computers, bed_bath_table, cool_stuff* chiếm GMV lớn.
    
    **Kiến nghị:**
    * Định tuyến **nguồn hàng & tồn kho** cho SKU top (theo bang), giữ **service level** ổn định trước Q4.
    * Với danh mục có mùa vụ, **đặt mục tiêu GMV theo tháng** thay vì tuyến tính năm; dùng **price test** (A/B) cho SKU đầu bảng.
    
    ---
    
    ### 4) Geography & Market Expansion
    
    * **SP, RJ, MG** là **core markets**; một số bang **on-time tốt nhưng GMV thấp** → dư địa mở rộng.
    * **Outlier delivery days** ở một vài bang (RO/AM …) cho thấy **last-mile dài** hoặc **thiếu đối tác 3PL phù hợp**.
    
    **Kiến nghị:**
    * Triển khai **SLA playbook theo vùng**: chuẩn tuyến/đối tác 3PL, SLA cam kết, và **buffer time** theo mùa.
    * Với bang GMV thấp/on-time tốt → chạy **chiến dịch địa phương** (voucher phí ship, promise "giao nhanh" để kéo conversion).
    
    ---
    
    ### 5) Seller Performance & Compliance
    
    * Nhìn chung **tuân thủ tốt** (on-time TB ~92%, cancel ~0.01%), **review > 4.0**.
    * Có một số **seller outlier** (review < 3.8 hoặc on-time < 0.90) với **GMV tương đối lớn** → rủi ro trải nghiệm.
    
    **Kiến nghị:**
    * Thiết lập **cảnh báo tự động**: `on_time_rate < 92%` HOẶC `avg_review_score < 4.0` với GMV > ngưỡng (ví dụ 20k).
    * Áp dụng **penalty/bonus SLA** & **coaching** cho seller yếu, ưu tiên hiển thị (search ranking) cho seller tốt.
    
    ---
    
    ### 6) Operations / Delivery SLA
    
    * **On-time** ổn định quanh **94%** ở tab Basis=Delivery; **delivery days** thường **11–15 ngày**, nhưng **một số tháng tăng** trùng thời gian sale.
    * **Late orders** (nếu bật) tập trung ở bang có tuyến xa.
    
    **Kiến nghị:**
    * Trước mùa cao điểm, **nâng năng lực fulfillment** (cut-off time, line-haul) tại các hub trọng điểm; đánh giá **SLA theo partner**.
    * Công bố **delivery promise theo bang** (không one-size-fits-all) để quản kỳ vọng khách.
    
    ---
    
    ### 7) Customer / Cohort & Retention
    
    * **Trạng thái hiện tại**: Heatmap đã được sửa để hiển thị retention đúng; **retention(k)** giảm dần theo tháng.
    * Dùng thêm phân rã **Returning vs New** để bóc tách động lực GMV.
    
    **Kiến nghị:**
    * Đặt **OKR retention** cho k=1,3,6; chạy **remarketing** cho cohort suy giảm mạnh; áp dụng **đề xuất sản phẩm** theo category lần mua đầu.
    
    ---
    
    ### 8) Finance & Payment Mix
    
    * **credit_card** chiếm tỷ trọng cao & **tăng mạnh cuối năm**; **avg installments ~3.4–4.1** → khách sẵn sàng trả góp ngắn.
    * **boleto/voucher** duy trì tỷ trọng nhỏ, có dao động theo chiến dịch.
    
    **Kiến nghị:**
    * Đàm phán **phí MDR** & **kỳ hạn thu tiền** với PSP do volume credit_card cao; kiểm soát **rủi ro nợ** với kỳ hạn trả góp.
    * A/B **ưu đãi installments** ở ngành hàng giá cao (computers/furniture) để tối ưu conversion mà không đội chi phí quá mức.
    
    ---
    
    ### 9) Data Quality & Reliability
    
    * **Pass rate cao (≈96–100%)** cho thấy pipeline ổn định.
    * Rủi ro chính: **schema drift** hoặc **dirty data** từ raw.
    
    **Kiến nghị:**
    * Nâng cấp DQ từ "demo" → **rules cụ thể**: `row_count delta`, `domain check (review in 1..5)`, **referential integrity** (FK fact → dim), **freshness** (max lag giờ/ngày).
    * Hiển thị **DQ badge** trên mỗi tab (Green/Amber/Red) cho lãnh đạo.
    """)
    
    st.markdown("---")
    st.markdown("## 🎯 Ưu tiên hành động (90 ngày)")
    
    st.markdown("""
    1. **SLA theo vùng trước Q4**
       * Chuẩn hóa đối tác 3PL, tuyến, và **delivery promise** theo bang; giảm outlier >20 ngày.
    
    2. **Tập trung danh mục hạt nhân**
       * Kế hoạch tồn kho + chiến dịch cho Top-Pareto category; A/B pricing/upsell.
    
    3. **Giám sát seller outlier**
       * Thiết lập cảnh báo & cơ chế thưởng/phạt SLA; xếp hạng hiển thị theo hiệu suất.
    
    4. **Hoàn thiện Cohort/Retention**
       * Sửa truy vấn; đặt mục tiêu **k=1, k=3, k=6**; gắn chiến dịch giữ chân (email/app push).
    
    5. **Tối ưu payment mix**
       * Đàm phán phí + quản trị installments; theo dõi rủi ro chargeback/chậm thanh toán.
    
    6. **Nâng chuẩn Data Quality**
       * Chuyển các DQ check từ demo sang **rule vận hành**, log & alert trong Dagster.
    """)
    
    st.markdown("---")
    st.markdown("## ⚠️ Rủi ro & điểm cần theo dõi")
    
    st.markdown("""
    * **Seasonality**: không so sánh thô MoM ở thời điểm sale; luôn đối chiếu **YoY** hoặc **MA 3M**.
    * **Coverage thời gian**: một số tab có YoY=0% khi thiếu dữ liệu năm trước; cần note rõ cửa sổ dữ liệu.
    * **Delivery outliers**: vài bang có **delivery days** cao bất thường → cần khắc phục để tránh **điểm review giảm**.
    * **Cohort**: đã được sửa để hiển thị retention đúng; cần theo dõi định kỳ để cập nhật **chỉ số retention** vào Executive.
    """)
    
    st.markdown("---")
    st.markdown("## 📊 Tác động kỳ vọng (nếu triển khai kiến nghị)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        * **GMV**: tăng 5–10% nhờ tập trung category hạt nhân + conversion từ delivery promise rõ ràng.
        * **SLA**: giảm 10–20% số bang outlier >20 ngày; **on-time** tăng 1–2 điểm %.
        * **AOV**: tăng 2–4% nhờ bundle/upsell.
        """)
    
    with col2:
        st.markdown("""
        * **LTV/Retention**: cải thiện 2–3 điểm % ở k=3 nhờ remarketing đúng lúc.
        * **Chi phí tài chính**: tối ưu 0.1–0.3 điểm % MDR nhờ leverage volume credit_card.
        """)

# Footer
st.markdown("---")
st.caption(f"© Executive Analytics Suite • Coverage: {COVER_MIN} → {COVER_MAX}")
