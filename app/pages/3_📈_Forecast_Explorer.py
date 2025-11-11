"""
Forecast Explorer - ML Forecasting Showcase
Interactive interface for exploring demand forecasts
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, datetime, timedelta
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os

# ====== Config ======
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

# Enhanced CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

:root{
  --bg:#0b1220; --bg2:#10192b; --card:#0f172a; --line:#1f2a44;
  --text:#e2e8f0; --muted:#94a3b8; --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; --pri:#22d3ee;
}

html, body, [class*=css] { font-family: 'Inter', sans-serif; }
.main .block-container{max-width:1400px;padding-top:1rem;padding-bottom:3rem}
h1,h2,h3{letter-spacing:.2px}
hr{border-color:var(--line);opacity:.4;margin:1rem 0}

.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px;transition:border-color 0.2s}
.card:hover{border-color:#294166}
.metric-card{border-radius:12px;padding:16px;background:var(--card);border-left:4px solid var(--pri);margin-bottom:8px;transition:all 0.2s}
.metric-card:hover{border-left-width:6px}

.badge{display:inline-flex;gap:6px;align-items:center;padding:4px 10px;border-radius:999px;
  background:rgba(34,211,238,.12);border:1px solid rgba(34,211,238,.25);font-size:12px}
.muted{color:var(--muted)}
.kpi{font-size:28px;font-weight:700}

.stButton>button, .stDownloadButton>button{border-radius:14px;padding:.8rem 1.1rem;font-weight:700;font-size:15px}
.section-title{display:flex;align-items:center;gap:10px;margin:1.5rem 0 1rem}
.section-title h3{margin:0}
.section-title:after{content:"";flex:1;height:1px;background:linear-gradient(90deg,transparent, var(--line))}

.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] { padding:10px 14px;border-radius:10px;background:var(--card);border:1px solid var(--line) }
.stTabs [data-baseweb="tab"][aria-selected="true"] { background:var(--bg2);border-color:var(--pri) }
</style>
""", unsafe_allow_html=True)

# ====== Helpers ======
@st.cache_data(ttl=600, show_spinner="Đang tải dữ liệu...")
def run_query(sql: str, schema: str = "platinum"):
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
            source="streamlit-forecast-explorer"
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        error_str = str(e)
        if "does not exist" in error_str or "TABLE_NOT_FOUND" in error_str:
            return pd.DataFrame()
        return pd.DataFrame()

@st.cache_data(ttl=1800, show_spinner="Đang tải danh sách sản phẩm...")
def load_product_options_with_names(schema_pl: str, schema_gd: str):
    """Load products with names for filter dropdown (cached for 30 minutes)"""
    try:
        products_sql = f"""
        SELECT DISTINCT
            f.product_id,
            COALESCE(pc.product_category_name_english, dp.product_category_name, f.product_id) AS product_name,
            COALESCE(pc.product_category_name_english, 'Unknown') AS category_en
        FROM {schema_pl}.demand_forecast f
        LEFT JOIN {schema_gd}.dim_product dp
          ON f.product_id = dp.product_id
        LEFT JOIN {schema_gd}.dim_product_category pc
          ON dp.product_category_name = pc.product_category_name
        WHERE f.product_id IS NOT NULL
        ORDER BY category_en, f.product_id
        LIMIT 200
        """
        products_df = run_query(products_sql, schema_pl)
        
        product_options = []
        if not products_df.empty:
            for _, row in products_df.iterrows():
                pid = row.get("product_id")
                pname = row.get("product_name") or pid or "Unknown"
                category = row.get("category_en") or "Unknown"
                
                # Create display name: "Category (Product ID short)"
                display_name = f"{category}"
                if len(display_name) > 35:
                    display_name = display_name[:32] + "..."
                
                # Add short product ID for uniqueness
                short_id = str(pid)[:8] if pid else ""
                display_name = f"{display_name} ({short_id})" if short_id else display_name
                
                product_options.append({
                    "id": pid,
                    "name": pname,
                    "category": category,
                    "display": display_name
                })
        
        return product_options
    except:
        return []

def check_table_exists(table_name: str, schema: str = SCHEMA_PL) -> bool:
    """Check if table exists without raising error"""
    try:
        sql = f"SELECT 1 FROM {schema}.{table_name} LIMIT 1"
        df = run_query(sql, schema)
        return not df.empty
    except:
        return False

# ====== UI ======
st.title("📈 Forecast Explorer")
st.caption("Khám phá dự báo nhu cầu sản phẩm với Machine Learning • Powered by LightGBM + MLflow + Delta Lake")

# Info Section
info_col1, info_col2, info_col3, info_col4 = st.columns(4)

with info_col1:
    st.markdown("""
    <div class='metric-card'>
    <div style='font-size:12px;opacity:.8;margin-bottom:4px'>Model</div>
    <div style='font-size:22px;font-weight:700;color:#22d3ee'>LightGBM</div>
    <div style='opacity:.6;font-size:11px'>Gradient Boosting</div>
    </div>
    """, unsafe_allow_html=True)

with info_col2:
    st.markdown("""
    <div class='metric-card'>
    <div style='font-size:12px;opacity:.8;margin-bottom:4px'>Forecast Horizon</div>
    <div style='font-size:22px;font-weight:700;color:#22c55e'>28 Ngày</div>
    <div style='opacity:.6;font-size:11px'>Recursive Roll-forward</div>
    </div>
    """, unsafe_allow_html=True)

with info_col3:
    st.markdown("""
    <div class='metric-card'>
    <div style='font-size:12px;opacity:.8;margin-bottom:4px'>Features</div>
    <div style='font-size:22px;font-weight:700;color:#f59e0b'>15+</div>
    <div style='opacity:.6;font-size:11px'>Lags, Rolling, Calendar</div>
    </div>
    """, unsafe_allow_html=True)

with info_col4:
    st.markdown("""
    <div class='metric-card'>
    <div style='font-size:12px;opacity:.8;margin-bottom:4px'>Storage</div>
    <div style='font-size:22px;font-weight:700;color:#8b5cf6'>Delta Lake</div>
    <div style='opacity:.6;font-size:11px'>ACID Transactions</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Main Content Tabs
tab_forecast, tab_monitoring, tab_mlflow, tab_info = st.tabs([
    "🔮 Dự báo", 
    "📊 Monitoring", 
    "🧪 MLflow",
    "ℹ️ Thông tin"
])

# ==============
# Tab: Forecast
# ==============
with tab_forecast:
    st.markdown("### 🔮 Dữ liệu dự báo")
    
    # Header phụ với thông tin model
    st.markdown("""
    <div style='background:rgba(34,211,238,0.1);border:1px solid rgba(34,211,238,0.3);border-radius:8px;padding:12px;margin-bottom:16px;font-size:13px;opacity:0.9'>
    💡 <strong>Giá trị dự báo (yhat)</strong> và <strong>khoảng tin cậy (yhat_lo/hi)</strong> sinh từ mô hình LightGBM; 
    <strong>horizon</strong> = ngày +n tiếp theo; <strong>Records</strong> = số dòng dự báo theo bộ lọc.
    </div>
    """, unsafe_allow_html=True)
    
    has_forecast_table = check_table_exists("demand_forecast", SCHEMA_PL)
    
    if not has_forecast_table:
        st.info("""
        📊 **Bảng `platinum.demand_forecast` chưa được tạo.**
        
        Để tạo dữ liệu forecast, vui lòng:
        1. Mở **Dagster UI** (http://localhost:3001)
        2. Vào **Jobs** → `forecast_job`
        3. Click **Launch Run**
        4. Đợi pipeline hoàn thành (4 bước: features → train → predict → monitor)
        5. Refresh trang này để xem kết quả
        
        Kết quả sẽ được lưu vào `platinum.demand_forecast`
        """)
        
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("[🚀 Mở Dagster UI](http://localhost:3001)")
        with col2:
            st.caption("💡 Pipeline sẽ chạy ~5-10 phút tùy dữ liệu")
    else:
        # Load available filters and model info
        try:
            # Load products with names (cached)
            product_options = load_product_options_with_names(SCHEMA_PL, SCHEMA_GD)
            
            regions_sql = f"SELECT DISTINCT region_id FROM {SCHEMA_PL}.demand_forecast WHERE region_id IS NOT NULL ORDER BY region_id LIMIT 50"
            regions_df = run_query(regions_sql, SCHEMA_PL)
            regions = regions_df["region_id"].tolist() if not regions_df.empty else []
            
            date_range_sql = f"SELECT MIN(forecast_date) AS min_date, MAX(forecast_date) AS max_date FROM {SCHEMA_PL}.demand_forecast"
            date_range_df = run_query(date_range_sql, SCHEMA_PL)
            if not date_range_df.empty:
                min_date = pd.to_datetime(date_range_df["min_date"].iloc[0]).date()
                max_date = pd.to_datetime(date_range_df["max_date"].iloc[0]).date()
            else:
                min_date = date.today() - timedelta(days=90)
                max_date = date.today()
            
            # Get model info (run_id, generated_at) if available
            try:
                # Check if columns exist first
                check_cols_sql = f"SELECT * FROM {SCHEMA_PL}.demand_forecast LIMIT 1"
                check_df = run_query(check_cols_sql, SCHEMA_PL)
                has_run_id = "run_id" in check_df.columns if not check_df.empty else False
                has_generated_at = "generated_at" in check_df.columns if not check_df.empty else False
                
                if has_run_id or has_generated_at:
                    cols = []
                    if "model_name" in check_df.columns:
                        cols.append("model_name")
                    if has_run_id:
                        cols.append("run_id")
                    if has_generated_at:
                        cols.append("generated_at")
                    
                    if cols:
                        where_parts = []
                        if has_run_id:
                            where_parts.append("run_id IS NOT NULL")
                        if has_generated_at:
                            where_parts.append("generated_at IS NOT NULL")
                        where_clause = " OR ".join(where_parts) if where_parts else "1=1"
                        order_by = "generated_at DESC" if has_generated_at else ("run_id DESC" if has_run_id else "model_name DESC")
                        
                        model_info_sql = f"""
                        SELECT DISTINCT {", ".join(cols)}
                        FROM {SCHEMA_PL}.demand_forecast
                        WHERE {where_clause}
                        ORDER BY {order_by}
                        LIMIT 1
                        """
                        model_info_df = run_query(model_info_sql, SCHEMA_PL)
                    else:
                        model_info_df = pd.DataFrame()
                else:
                    model_info_df = pd.DataFrame()
            except:
                model_info_df = pd.DataFrame()
        except:
            product_options = []
            regions = []
            min_date = date.today() - timedelta(days=90)
            max_date = date.today()
            model_info_df = pd.DataFrame()
        
        # Badge thông tin model
        if not model_info_df.empty:
            model_name = model_info_df["model_name"].iloc[0] if "model_name" in model_info_df.columns else "LightGBM"
            run_id = model_info_df["run_id"].iloc[0] if "run_id" in model_info_df.columns and pd.notna(model_info_df["run_id"].iloc[0]) else None
            generated_at = model_info_df["generated_at"].iloc[0] if "generated_at" in model_info_df.columns and pd.notna(model_info_df["generated_at"].iloc[0]) else None
            
            badge_col1, badge_col2, badge_col3 = st.columns(3)
            with badge_col1:
                st.markdown(f"""
                <div class='badge' style='margin-bottom:12px'>
                <span>🤖 Model:</span> <strong>{model_name}</strong>
                </div>
                """, unsafe_allow_html=True)
            if run_id:
                with badge_col2:
                    st.markdown(f"""
                    <div class='badge' style='margin-bottom:12px'>
                    <span>🆔 Run ID:</span> <strong>{run_id}</strong>
                    </div>
                    """, unsafe_allow_html=True)
            if generated_at:
                with badge_col3:
                    gen_str = pd.to_datetime(generated_at).strftime("%Y-%m-%d %H:%M") if pd.notna(generated_at) else "N/A"
                    st.markdown(f"""
                    <div class='badge' style='margin-bottom:12px'>
                    <span>📅 Generated:</span> <strong>{gen_str}</strong>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Filters với tooltips
        st.markdown("#### 🔍 Bộ lọc")
        with st.expander("ℹ️ Hướng dẫn sử dụng bộ lọc", expanded=False):
            st.markdown("""
            - **Product Name / Category**: Chọn chuỗi dự báo theo tên sản phẩm/danh mục. "Tất cả" = tổng hợp tất cả series.
              Tên hiển thị là danh mục sản phẩm (category) kèm Product ID ngắn trong ngoặc để đảm bảo chính xác.
            - **Region ID**: Chọn khu vực cụ thể hoặc "Tất cả" để xem tổng hợp.
            - **Horizon (ngày)**: Số ngày dự báo tính từ ngày gần nhất có dữ liệu thật; H=1 là ngày kế tiếp, H=7 là ngày thứ 7.
            - **Khoảng ngày**: Filter theo `forecast_date` (ngày được dự báo).
            """)
        
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        with filter_col1:
            # Create selectbox options
            product_select_options = [{"id": None, "display": "Tất cả", "category": "All"}]
            if product_options:
                product_select_options.extend(product_options)
            
            selected_product_option = st.selectbox(
                "📦 Product Name / Category",
                product_select_options,
                index=0,
                format_func=lambda x: x["display"],
                help="Chọn sản phẩm theo tên danh mục (category). Product ID hiển thị trong ngoặc để đảm bảo chính xác."
            )
            selected_product_id = selected_product_option.get("id") if selected_product_option else None
            selected_product_name = selected_product_option.get("name") if selected_product_option else None
            selected_product_category = selected_product_option.get("category") if selected_product_option else None
            
        with filter_col2:
            selected_region = st.selectbox("🌍 Region ID", ["Tất cả"] + regions, index=0, help="Chọn khu vực cụ thể hoặc 'Tất cả' để xem tổng hợp")
        with filter_col3:
            selected_horizon = st.slider("📅 Horizon (ngày)", 1, 28, (1, 7), help="Số ngày dự báo: H=1 là ngày kế tiếp, H=7 là ngày thứ 7")
        
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            start_date = st.date_input("Ngày bắt đầu", min_value=min_date, max_value=max_date, value=min_date, help="Ngày bắt đầu của khoảng forecast_date")
        with date_col2:
            end_date = st.date_input("Ngày kết thúc", min_value=min_date, max_value=max_date, value=max_date, help="Ngày kết thúc của khoảng forecast_date")
        
        # Build query with JOIN to get product names
        where_conditions = []
        if selected_product_id:
            where_conditions.append(f"f.product_id = '{selected_product_id}'")
        if selected_region != "Tất cả":
            where_conditions.append(f"f.region_id = '{selected_region}'")
        where_conditions.append(f"f.horizon >= {selected_horizon[0]} AND f.horizon <= {selected_horizon[1]}")
        where_conditions.append(f"f.forecast_date >= DATE '{start_date}' AND f.forecast_date <= DATE '{end_date}'")
        where_clause = " AND ".join(where_conditions)
        
        # Build SELECT with available columns and JOIN for product names
        try:
            check_cols_sql = f"SELECT * FROM {SCHEMA_PL}.demand_forecast LIMIT 1"
            check_df = run_query(check_cols_sql, SCHEMA_PL)
            base_cols = ["f.forecast_date", "f.product_id", "f.region_id", "f.horizon", "f.yhat", "f.yhat_lo", "f.yhat_hi"]
            optional_cols = []
            if not check_df.empty:
                if "model_name" in check_df.columns:
                    optional_cols.append("f.model_name")
                if "run_id" in check_df.columns:
                    optional_cols.append("f.run_id")
                if "generated_at" in check_df.columns:
                    optional_cols.append("f.generated_at")
            
            # Add product name columns
            product_cols = [
                "COALESCE(pc.product_category_name_english, dp.product_category_name, f.product_id) AS product_name",
                "COALESCE(pc.product_category_name_english, 'Unknown') AS category_en"
            ]
            
            select_cols = ", ".join(base_cols + optional_cols + product_cols)
        except:
            select_cols = "f.forecast_date, f.product_id, f.region_id, f.horizon, f.yhat, f.yhat_lo, f.yhat_hi, f.model_name, COALESCE(pc.product_category_name_english, dp.product_category_name, f.product_id) AS product_name, COALESCE(pc.product_category_name_english, 'Unknown') AS category_en"
        
        forecast_sql = f"""
        SELECT {select_cols}
        FROM {SCHEMA_PL}.demand_forecast f
        LEFT JOIN {SCHEMA_GD}.dim_product dp
          ON f.product_id = dp.product_id
        LEFT JOIN {SCHEMA_GD}.dim_product_category pc
          ON dp.product_category_name = pc.product_category_name
        WHERE {where_clause}
        ORDER BY f.forecast_date, f.horizon, product_name, f.product_id
        LIMIT 1000
        """
        
        forecast_df = run_query(forecast_sql, SCHEMA_PL)
        
        if not forecast_df.empty:
            st.success(f"✅ Đã tải {len(forecast_df):,} dòng dự báo")
            
            # KPIs với tooltips
            st.markdown("#### 📊 Tổng quan")
            with st.expander("ℹ️ Giải thích các chỉ số", expanded=False):
                st.markdown("""
                - **Forecast TB**: Giá trị dự báo trung bình (trên tất cả series & horizons đang lọc). Dùng để nhìn "mặt bằng" nhu cầu.
                - **Forecast Max / Min**: Cực trị trong tập đang lọc → phát hiện series "nóng/lạnh".
                - **Records**: Số dòng dự báo đã nạp = **số series × số horizons**. Giúp ước lượng coverage (độ phủ) của hệ thống.
                """)
            
            kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
            with kpi_col1:
                st.metric("Forecast TB", f"{forecast_df['yhat'].mean():,.0f}", help="Giá trị dự báo trung bình")
            with kpi_col2:
                st.metric("Forecast Max", f"{forecast_df['yhat'].max():,.0f}", help="Giá trị dự báo cao nhất")
            with kpi_col3:
                st.metric("Forecast Min", f"{forecast_df['yhat'].min():,.0f}", help="Giá trị dự báo thấp nhất")
            with kpi_col4:
                st.metric("Records", f"{len(forecast_df):,}", help="Số dòng dự báo = số series × số horizons")
            
            # KPI Strips với Scenario (Low/Base/High)
            st.markdown("---")
            st.markdown("#### 📈 Kịch bản dự báo (Planning)")
            with st.expander("ℹ️ Giải thích kịch bản", expanded=False):
                st.markdown("""
                - **Kịch bản thận trọng (Low)**: Tổng yhat_lo - dùng cho kế hoạch "worst case"
                - **Kịch bản cơ sở (Base)**: Tổng yhat - dự báo trung bình
                - **Kịch bản lạc quan (High)**: Tổng yhat_hi - dùng cho kế hoạch "best case"
                """)
            
            scenario_df = forecast_df.groupby("forecast_date").agg({
                "yhat_lo": "sum",
                "yhat": "sum",
                "yhat_hi": "sum"
            }).reset_index()
            
            scenario_col1, scenario_col2, scenario_col3 = st.columns(3)
            with scenario_col1:
                sum_lo = scenario_df["yhat_lo"].sum()
                st.metric("🔴 Kịch bản thận trọng", f"{sum_lo:,.0f}", delta=f"{sum_lo - forecast_df['yhat'].sum():,.0f}", delta_color="inverse", help="Tổng yhat_lo - worst case scenario")
            with scenario_col2:
                sum_base = scenario_df["yhat"].sum()
                st.metric("🟡 Kịch bản cơ sở", f"{sum_base:,.0f}", help="Tổng yhat - base scenario")
            with scenario_col3:
                sum_hi = scenario_df["yhat_hi"].sum()
                st.metric("🟢 Kịch bản lạc quan", f"{sum_hi:,.0f}", delta=f"{sum_hi - forecast_df['yhat'].sum():,.0f}", help="Tổng yhat_hi - best case scenario")
            
            st.markdown("---")
            st.markdown("#### 📈 Biểu đồ Forecast")
            
            # Chart: Forecast Trend với tooltip
            with st.expander("ℹ️ Giải thích biểu đồ Forecast Trend", expanded=False):
                st.markdown("""
                - **Đường Forecast (yhat)**: Điểm dự báo trung bình cho ngày đó (sau khi gộp theo filter).
                - **Dải CI (yhat_lo / yhat_hi)**: Biên dưới/trên ±15% quanh dự báo (ước lượng bất định). 
                  CI hẹp → mô hình tự tin hơn; CI rộng → nên thận trọng khi ra quyết định.
                - **Xu hướng**: Dốc xuống/lên nhẹ cho thấy biến động ngắn hạn trong tuần tới.
                """)
            
            if "forecast_date" in forecast_df.columns:
                # Show product/category info in title if filtered
                chart_title = "Forecast Trend với Confidence Interval"
                if selected_product_id and selected_product_category:
                    chart_title += f" - {selected_product_category}"
                elif selected_product_name:
                    chart_title += f" - {selected_product_name[:30]}"
                
                trend_df = forecast_df.groupby("forecast_date").agg({"yhat": "mean", "yhat_lo": "mean", "yhat_hi": "mean"}).reset_index().sort_values("forecast_date")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=trend_df["forecast_date"], 
                    y=trend_df["yhat_hi"], 
                    mode="lines", 
                    name="Upper CI", 
                    line=dict(width=0), 
                    showlegend=False,
                    hovertemplate="Upper CI: %{y:,.0f}<extra></extra>"
                ))
                fig.add_trace(go.Scatter(
                    x=trend_df["forecast_date"], 
                    y=trend_df["yhat_lo"], 
                    mode="lines", 
                    name="Lower CI", 
                    line=dict(width=0), 
                    fillcolor="rgba(34,211,238,0.2)", 
                    fill="tonexty", 
                    showlegend=True,
                    hovertemplate="Lower CI: %{y:,.0f}<extra></extra>"
                ))
                fig.add_trace(go.Scatter(
                    x=trend_df["forecast_date"], 
                    y=trend_df["yhat"], 
                    mode="lines+markers", 
                    name="Forecast", 
                    line=dict(color="#22d3ee", width=2),
                    hovertemplate="Date: %{x|%Y-%m-%d}<br>Forecast: %{y:,.0f}<extra></extra>"
                ))
                fig.update_layout(
                    title=chart_title,
                    xaxis_title="Ngày",
                    yaxis_title="Forecast Value",
                    height=400,
                    template="plotly_dark",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Chart: Forecast by Horizon với tooltip
            if "horizon" in forecast_df.columns:
                with st.expander("ℹ️ Giải thích biểu đồ Forecast theo Horizon", expanded=False):
                    st.markdown("""
                    Giá trị trung bình theo từng horizon (1→7). Nếu cột H càng xa càng giảm mạnh → có thể có **sai số tích lũy** 
                    của dự báo "cuốn chiếu"; cân nhắc dùng H ngắn hơn cho quyết định tồn kho.
                    """)
                
                horizon_df = forecast_df.groupby("horizon").agg({"yhat": "mean"}).reset_index()
                
                # Add product info to title if filtered
                horizon_title = "Forecast theo Horizon"
                if selected_product_id and selected_product_category:
                    horizon_title += f" - {selected_product_category}"
                
                fig_h = px.bar(
                    horizon_df, 
                    x="horizon", 
                    y="yhat", 
                    title=horizon_title, 
                    labels={"horizon": "Horizon (ngày)", "yhat": "Forecast TB"},
                    hover_data={"yhat": ":,.0f"}
                )
                fig_h.update_layout(
                    height=350, 
                    template="plotly_dark", 
                    plot_bgcolor="rgba(0,0,0,0)", 
                    paper_bgcolor="rgba(0,0,0,0)"
                )
                fig_h.update_traces(
                    hovertemplate="Horizon: %{x} ngày<br>Forecast: %{y:,.0f}<extra></extra>"
                )
                st.plotly_chart(fig_h, use_container_width=True)
                
                # Tính độ dốc (decay) H1 vs H7
                try:
                    if len(horizon_df) >= 2:
                        h1_mask = horizon_df["horizon"] == selected_horizon[0]
                        h7_mask = horizon_df["horizon"] == selected_horizon[1]
                        h1_val = horizon_df[h1_mask]["yhat"].iloc[0] if h1_mask.any() else None
                        h7_val = horizon_df[h7_mask]["yhat"].iloc[0] if h7_mask.any() else None
                        if h1_val is not None and h7_val is not None and h1_val > 0:
                            decay_pct = ((h7_val - h1_val) / h1_val) * 100
                            decay_color = "🔴" if decay_pct < -10 else "🟡" if decay_pct < -5 else "🟢"
                            st.caption(f"{decay_color} **Horizon decay**: H{selected_horizon[1]} so với H{selected_horizon[0]}: {decay_pct:.1f}%. Nếu giảm >10%, cân nhắc rút ngắn horizon cho quyết định tồn kho.")
                except:
                    pass  # Skip if calculation fails
            
            # Section: Rủi ro (CI width)
            st.markdown("---")
            st.markdown("#### ⚠️ Rủi ro dự báo (Độ bất định)")
            with st.expander("ℹ️ Giải thích độ bất định", expanded=False):
                st.markdown("""
                Series có **CI rộng** (yhat_hi - yhat_lo) cao cho thấy mô hình ít tự tin vào dự báo.
                Nên giám sát các series này và cân nhắc thêm dữ liệu hoặc điều chỉnh mô hình.
                """)
            
            forecast_df["ci_width"] = forecast_df["yhat_hi"] - forecast_df["yhat_lo"]
            ci_avg = forecast_df["ci_width"].mean()
            
            risk_col1, risk_col2 = st.columns(2)
            with risk_col1:
                st.metric("CI Width TB", f"{ci_avg:,.0f}", help="Khoảng tin cậy trung bình (yhat_hi - yhat_lo)")
            
            with risk_col2:
                st.markdown("**Top 10 series có CI rộng nhất**")
                try:
                    # Group by product_name and region_id for better readability
                    group_cols = ["product_name", "category_en", "product_id", "region_id"]
                    available_cols = [col for col in group_cols if col in forecast_df.columns]
                    if available_cols:
                        top_ci_series = forecast_df.groupby(available_cols)["ci_width"].mean().reset_index().sort_values("ci_width", ascending=False).head(10)
                        # Reorder columns for display: name first, then ID
                        display_cols = []
                        if "product_name" in top_ci_series.columns:
                            display_cols.append("product_name")
                        if "category_en" in top_ci_series.columns:
                            display_cols.append("category_en")
                        if "product_id" in top_ci_series.columns:
                            display_cols.append("product_id")
                        if "region_id" in top_ci_series.columns:
                            display_cols.append("region_id")
                        display_cols.append("ci_width")
                        top_ci_series = top_ci_series[display_cols]
                    else:
                        top_ci_series = forecast_df.groupby(["product_id", "region_id"])["ci_width"].mean().reset_index().sort_values("ci_width", ascending=False).head(10)
                    
                    if not top_ci_series.empty:
                        st.dataframe(top_ci_series, use_container_width=True, height=200)
                    else:
                        st.info("Không có dữ liệu")
                except Exception as e:
                    st.info(f"Không thể tính toán: {e}")
            
            # Chart: CI width theo ngày
            if "forecast_date" in forecast_df.columns:
                ci_trend_df = forecast_df.groupby("forecast_date")["ci_width"].mean().reset_index().sort_values("forecast_date")
                fig_ci = px.line(ci_trend_df, x="forecast_date", y="ci_width", title="CI Width theo ngày", labels={"ci_width": "CI Width TB", "forecast_date": "Ngày"})
                fig_ci.update_layout(height=300, template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_ci, use_container_width=True)
            
            # Section: Top/Bottom movers
            st.markdown("---")
            st.markdown("#### 🎯 Top/Bottom Movers (Ưu tiên vận hành)")
            with st.expander("ℹ️ Giải thích Top/Bottom Movers", expanded=False):
                st.markdown("""
                Bảng xếp hạng theo **đóng góp dự báo** (tổng yhat) để biết sản phẩm/khu vực nào kéo chính.
                - **Top series** kéo trung bình lên (gần Max) nên ưu tiên tồn kho/marketing.
                - **Bottom series** (gần Min) rà soát tránh overstock.
                """)
            
            # Group by product_name if available, otherwise product_id
            group_cols = []
            if "product_name" in forecast_df.columns:
                group_cols = ["product_name", "category_en", "product_id", "region_id"]
            elif "category_en" in forecast_df.columns:
                group_cols = ["category_en", "product_id", "region_id"]
            else:
                group_cols = ["product_id", "region_id"]
            
            movers_df = forecast_df.groupby(group_cols).agg({
                "yhat": "sum",
                "ci_width": "mean"
            }).reset_index().sort_values("yhat", ascending=False)
            
            # Reorder columns for display: name/category first
            display_cols = []
            if "product_name" in movers_df.columns:
                display_cols.append("product_name")
            if "category_en" in movers_df.columns:
                display_cols.append("category_en")
            if "product_id" in movers_df.columns:
                display_cols.append("product_id")
            if "region_id" in movers_df.columns:
                display_cols.append("region_id")
            display_cols.extend(["yhat", "ci_width"])
            movers_df = movers_df[display_cols]
            
            # Rename columns for better display
            movers_df = movers_df.rename(columns={
                "product_name": "Product Name",
                "category_en": "Category",
                "product_id": "Product ID",
                "region_id": "Region ID",
                "yhat": "Sum Forecast",
                "ci_width": "Avg CI Width"
            })
            
            movers_col1, movers_col2 = st.columns(2)
            with movers_col1:
                st.markdown("**🔝 Top 10 (Cao nhất)**")
                st.dataframe(movers_df.head(10), use_container_width=True, height=300)
            with movers_col2:
                st.markdown("**🔻 Bottom 10 (Thấp nhất)**")
                st.dataframe(movers_df.tail(10), use_container_width=True, height=300)
            
            # Section: Pareto 80/20
            st.markdown("---")
            st.markdown("#### 📊 Phân tích Pareto (80/20)")
            with st.expander("ℹ️ Giải thích Pareto", expanded=False):
                st.markdown("""
                Xem **bao nhiêu series** chiếm **80%** tổng forecast. Giúp tập trung nguồn lực vào nhóm series quan trọng nhất.
                """)
            
            # Use movers_df before renaming for calculations
            movers_df_calc = forecast_df.groupby(group_cols).agg({
                "yhat": "sum",
                "ci_width": "mean"
            }).reset_index().sort_values("yhat", ascending=False)
            
            total_forecast = movers_df_calc["yhat"].sum()
            movers_df_calc["cumulative"] = movers_df_calc["yhat"].cumsum()
            movers_df_calc["cumulative_pct"] = (movers_df_calc["cumulative"] / total_forecast) * 100 if total_forecast > 0 else 0
            
            n_80pct = len(movers_df_calc[movers_df_calc["cumulative_pct"] <= 80])
            pct_80pct = (n_80pct / len(movers_df_calc)) * 100 if len(movers_df_calc) > 0 else 0
            
            pareto_col1, pareto_col2, pareto_col3 = st.columns(3)
            with pareto_col1:
                st.metric("Số series cho 80%", f"{n_80pct}", help="Số series chiếm 80% tổng forecast")
            with pareto_col2:
                st.metric("Tỷ lệ series", f"{pct_80pct:.1f}%", help="Phần trăm series trong tổng số")
            with pareto_col3:
                st.metric("Tổng forecast", f"{total_forecast:,.0f}", help="Tổng giá trị dự báo")
            
            # Chart: Pareto chart
            try:
                top_20 = movers_df_calc.head(20)
                if len(top_20) > 0:
                    # Create x-axis labels with product name/category
                    x_labels = []
                    for _, row in top_20.iterrows():
                        if "product_name" in row and pd.notna(row["product_name"]):
                            label = f"{row['product_name'][:30]}"  # Truncate long names
                        elif "category_en" in row and pd.notna(row["category_en"]):
                            label = f"{row['category_en'][:30]}"
                        else:
                            label = str(row.get("product_id", "Unknown"))[:20]
                        if "region_id" in row:
                            label += f" × {row['region_id']}"
                        x_labels.append(label)
                    
                    fig_pareto = go.Figure()
                    fig_pareto.add_trace(go.Bar(
                        x=x_labels,
                        y=top_20["yhat"].values,
                        name="Forecast",
                        marker_color="#22d3ee"
                    ))
                    
                    # Calculate cumulative percentage for top 20
                    cumulative = top_20["yhat"].cumsum()
                    cumulative_pct = (cumulative / total_forecast) * 100
                    
                    fig_pareto.add_trace(go.Scatter(
                        x=x_labels,
                        y=cumulative_pct.values,
                        name="Cumulative %",
                        yaxis="y2",
                        line=dict(color="#f59e0b", width=2)
                    ))
                    fig_pareto.update_layout(
                        title="Pareto Chart - Top 20 Series",
                        xaxis_title="Series (Product × Region)",
                        yaxis_title="Forecast Value",
                        yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 100]),
                        height=400,
                        template="plotly_dark",
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(tickangle=-45)
                    )
                    st.plotly_chart(fig_pareto, use_container_width=True)
            except Exception as e:
                st.warning(f"Không thể vẽ Pareto chart: {e}")
            
            # Section: Khai thác nâng cao (SQL mẫu)
            st.markdown("---")
            with st.expander("🔧 Khai thác nâng cao - SQL Mẫu", expanded=False):
                st.markdown("### 📝 Các truy vấn SQL mẫu để khai thác sâu dữ liệu forecast")
                
                sql_tabs = st.tabs([
                    "1. Tổng cung - Kịch bản",
                    "2. Top/Bottom Movers",
                    "3. Độ bất định (CI)",
                    "4. Horizon Decay",
                    "5. Pareto 80/20",
                    "6. Phân bố theo vùng"
                ])
                
                with sql_tabs[0]:
                    st.markdown("**Tổng cung theo kịch bản (Low/Base/High)**")
                    st.code(f"""
-- Sum theo kịch bản
SELECT
  date(f.forecast_date) AS d,
  SUM(f.yhat_lo) AS sum_lo,
  SUM(f.yhat)    AS sum_base,
  SUM(f.yhat_hi) AS sum_hi
FROM {SCHEMA_PL}.demand_forecast f
WHERE f.forecast_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  {f"AND f.product_id = '{selected_product_id}'" if selected_product_id else ""}
  {f"AND f.region_id = '{selected_region}'" if selected_region != "Tất cả" else ""}
GROUP BY 1
ORDER BY 1;
                    """, language="sql")
                
                with sql_tabs[1]:
                    st.markdown("**Top 10 product × region đóng góp forecast**")
                    st.code(f"""
-- Top 10 product × region đóng góp forecast (với product name)
SELECT 
    COALESCE(pc.product_category_name_english, dp.product_category_name, f.product_id) AS product_name,
    f.product_id,
    f.region_id,
    SUM(f.yhat) AS sum_forecast
FROM {SCHEMA_PL}.demand_forecast f
LEFT JOIN {SCHEMA_GD}.dim_product dp
  ON f.product_id = dp.product_id
LEFT JOIN {SCHEMA_GD}.dim_product_category pc
  ON dp.product_category_name = pc.product_category_name
WHERE f.forecast_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  {f"AND f.product_id = '{selected_product_id}'" if selected_product_id else ""}
  {f"AND f.region_id = '{selected_region}'" if selected_region != "Tất cả" else ""}
GROUP BY 1, 2, 3
ORDER BY sum_forecast DESC
LIMIT 10;
                    """, language="sql")
                
                with sql_tabs[2]:
                    st.markdown("**Series có CI trung bình rộng nhất**")
                    st.code(f"""
-- Series có CI trung bình rộng nhất (với product name)
SELECT 
    COALESCE(pc.product_category_name_english, dp.product_category_name, f.product_id) AS product_name,
    f.product_id,
    f.region_id,
    AVG(f.yhat_hi - f.yhat_lo) AS avg_ci_width
FROM {SCHEMA_PL}.demand_forecast f
LEFT JOIN {SCHEMA_GD}.dim_product dp
  ON f.product_id = dp.product_id
LEFT JOIN {SCHEMA_GD}.dim_product_category pc
  ON dp.product_category_name = pc.product_category_name
WHERE f.forecast_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  {f"AND f.product_id = '{selected_product_id}'" if selected_product_id else ""}
  {f"AND f.region_id = '{selected_region}'" if selected_region != "Tất cả" else ""}
GROUP BY 1, 2, 3
ORDER BY avg_ci_width DESC
LIMIT 20;
                    """, language="sql")
                
                with sql_tabs[3]:
                    st.markdown("**Độ dốc (H1 vs H7) theo series**")
                    st.code(f"""
-- Độ dốc (H1 vs H7) theo series (với product name)
WITH by_h AS (
  SELECT 
    f.product_id,
    f.region_id,
    f.horizon,
    AVG(f.yhat) AS avg_yhat
  FROM {SCHEMA_PL}.demand_forecast f
  WHERE f.forecast_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {f"AND f.product_id = '{selected_product_id}'" if selected_product_id else ""}
    {f"AND f.region_id = '{selected_region}'" if selected_region != "Tất cả" else ""}
  GROUP BY 1,2,3
)
SELECT
  COALESCE(pc.product_category_name_english, dp.product_category_name, h.product_id) AS product_name,
  h.product_id,
  h.region_id,
  MAX(CASE WHEN h.horizon=1 THEN h.avg_yhat END) AS h1,
  MAX(CASE WHEN h.horizon=7 THEN h.avg_yhat END) AS h7,
  100.0 * (MAX(CASE WHEN h.horizon=7 THEN h.avg_yhat END) - MAX(CASE WHEN h.horizon=1 THEN h.avg_yhat END))
  / NULLIF(MAX(CASE WHEN h.horizon=1 THEN h.avg_yhat END),0) AS decay_pct
FROM by_h h
LEFT JOIN {SCHEMA_GD}.dim_product dp
  ON h.product_id = dp.product_id
LEFT JOIN {SCHEMA_GD}.dim_product_category pc
  ON dp.product_category_name = pc.product_category_name
GROUP BY 1, 2, 3
ORDER BY decay_pct ASC  -- âm mạnh = giảm mạnh theo H
LIMIT 20;
                    """, language="sql")
                
                with sql_tabs[4]:
                    st.markdown("**Pareto 80/20 - Số series chiếm 80% forecast**")
                    st.code(f"""
-- Pareto 80/20 (với product name)
WITH agg AS (
  SELECT 
    f.product_id,
    f.region_id,
    SUM(f.yhat) AS sum_forecast
  FROM {SCHEMA_PL}.demand_forecast f
  WHERE f.forecast_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {f"AND f.product_id = '{selected_product_id}'" if selected_product_id else ""}
    {f"AND f.region_id = '{selected_region}'" if selected_region != "Tất cả" else ""}
  GROUP BY 1,2
),
ranked AS (
  SELECT *, SUM(sum_forecast) OVER() AS total_all,
           SUM(sum_forecast) OVER(ORDER BY sum_forecast DESC
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sum
  FROM agg
)
SELECT
  COUNT(*) FILTER (WHERE cum_sum <= 0.8 * total_all) AS n_series_for_80pct,
  SUM(sum_forecast) AS total
FROM ranked;
                    """, language="sql")
                
                with sql_tabs[5]:
                    st.markdown("**Tổng forecast theo region_id**")
                    st.code(f"""
-- Tổng forecast theo region_id (để vẽ choropleth/map)
SELECT f.region_id, SUM(f.yhat) AS sum_forecast
FROM {SCHEMA_PL}.demand_forecast f
WHERE f.forecast_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  {f"AND f.product_id = '{selected_product_id}'" if selected_product_id else ""}
GROUP BY 1
ORDER BY sum_forecast DESC;
                    """, language="sql")
            
            st.markdown("---")
            st.markdown("#### 📋 Dữ liệu chi tiết")
            
            # Reorder columns for display: product_name/category first
            display_df = forecast_df.copy()
            display_cols = []
            if "product_name" in display_df.columns:
                display_cols.append("product_name")
            if "category_en" in display_df.columns:
                display_cols.append("category_en")
            if "product_id" in display_df.columns:
                display_cols.append("product_id")
            if "region_id" in display_df.columns:
                display_cols.append("region_id")
            if "forecast_date" in display_df.columns:
                display_cols.append("forecast_date")
            if "horizon" in display_df.columns:
                display_cols.append("horizon")
            if "yhat" in display_df.columns:
                display_cols.append("yhat")
            if "yhat_lo" in display_df.columns:
                display_cols.append("yhat_lo")
            if "yhat_hi" in display_df.columns:
                display_cols.append("yhat_hi")
            if "ci_width" in display_df.columns:
                display_cols.append("ci_width")
            # Add remaining columns
            for col in display_df.columns:
                if col not in display_cols:
                    display_cols.append(col)
            
            display_df = display_df[display_cols]
            
            # Rename columns for better display
            display_df = display_df.rename(columns={
                "product_name": "Product Name",
                "category_en": "Category",
                "product_id": "Product ID",
                "region_id": "Region ID",
                "forecast_date": "Forecast Date",
                "horizon": "Horizon",
                "yhat": "Forecast (yhat)",
                "yhat_lo": "Forecast Low",
                "yhat_hi": "Forecast High",
                "ci_width": "CI Width"
            })
            
            st.dataframe(display_df, use_container_width=True, height=400)
            
            csv = display_df.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Tải CSV", csv, f"demand_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv", use_container_width=True)
        else:
            st.info("📊 Không có dữ liệu forecast cho bộ lọc đã chọn.")

# ==============
# Tab: Monitoring
# ==============
with tab_monitoring:
    st.markdown("### 📊 Forecast Monitoring")
    
    has_monitoring_table = check_table_exists("forecast_monitoring", SCHEMA_PL)
    
    if not has_monitoring_table:
        st.info("📊 **Bảng `platinum.forecast_monitoring` chưa được tạo.** Bảng này được tạo tự động khi chạy `forecast_job`.")
    else:
        st.info("**Metrics**: sMAPE (càng thấp càng tốt), MAE, RMSE. So sánh actual vs forecast để đánh giá độ chính xác model.")
        
        mon_sql = f"""
        SELECT date, product_id, region_id, y_actual, yhat, abs_error, pct_error, smape, model_name
        FROM {SCHEMA_PL}.forecast_monitoring
        ORDER BY date DESC
        LIMIT 500
        """
        mon_df = run_query(mon_sql, SCHEMA_PL)
        
        if not mon_df.empty:
            st.success(f"✅ Đã tải {len(mon_df):,} dòng monitoring")
            
            avg_smape = mon_df['smape'].mean() if 'smape' in mon_df.columns else 0
            avg_mae = mon_df['abs_error'].mean() if 'abs_error' in mon_df.columns else 0
            avg_rmse = ((mon_df['abs_error']**2).mean()**0.5) if 'abs_error' in mon_df.columns else 0
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Avg sMAPE", f"{avg_smape:.2f}%")
            m2.metric("Avg MAE", f"{avg_mae:.2f}")
            m3.metric("Avg RMSE", f"{avg_rmse:.2f}")
            
            st.markdown("---")
            st.markdown("#### 📈 Biểu đồ Monitoring")
            
            # Actual vs Forecast
            if "date" in mon_df.columns and "y_actual" in mon_df.columns:
                chart_df = mon_df.groupby("date").agg({"y_actual": "sum", "yhat": "sum"}).reset_index().sort_values("date")
                fig_act = go.Figure()
                fig_act.add_trace(go.Scatter(x=chart_df["date"], y=chart_df["y_actual"], mode="lines+markers", name="Actual", line=dict(color="#22c55e", width=2)))
                fig_act.add_trace(go.Scatter(x=chart_df["date"], y=chart_df["yhat"], mode="lines+markers", name="Forecast", line=dict(color="#22d3ee", width=2, dash="dash")))
                fig_act.update_layout(title="Actual vs Forecast", xaxis_title="Ngày", yaxis_title="Value", height=400, template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_act, use_container_width=True)
            
            st.markdown("---")
            st.dataframe(mon_df, use_container_width=True, height=400)
            
            csv = mon_df.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Tải CSV", csv, f"forecast_monitoring_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv", use_container_width=True)
        else:
            st.info("📊 Không có dữ liệu monitoring.")

# ==============
# Tab: MLflow
# ==============
with tab_mlflow:
    st.markdown("### 🧪 MLflow Experiment Tracking")
    st.markdown("**MLflow** được sử dụng để: Track experiments & hyperparameters, Log model metrics (RMSE, MAE, sMAPE, R2), Store model artifacts, Version models")
    st.markdown("**Model Registry**: Model name: `lightgbm_global_recursive`, Storage: MinIO (S3-compatible), Tracking DB: MySQL")
    
    st.markdown("---")
    st.markdown("### 📦 Model Details")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Features**: Lagged values (1,7,28 days), Rolling averages (7,28 days), Calendar features (dow, month, is_weekend), Product & region embeddings")
    with col2:
        st.markdown("**Hyperparameters**: `num_leaves`: 31, `learning_rate`: 0.05, `n_estimators`: 100, `max_depth`: -1")
    
    st.info("💡 Tip: Mở MLflow UI để xem chi tiết experiments (nếu đã setup)")

# ==============
# Tab: Info
# ==============
with tab_info:
    st.markdown("### ℹ️ Thông tin hệ thống")
    st.markdown("#### 🏗️ Kiến trúc")
    st.markdown("**Data Flow**: Gold Layer → Feature Engineering → Silver Layer → Model Training → Batch Prediction → Platinum Layer → Monitoring")
    st.markdown("---")
    st.markdown("#### 🚀 Cách sử dụng")
    st.markdown("1. Chạy Forecast Pipeline: Mở Dagster UI → Jobs → forecast_job → Launch Run")
    st.markdown("2. Xem kết quả: Tab 'Dự báo' xem forecasts, Tab 'Monitoring' xem accuracy metrics")
    st.markdown("3. Export dữ liệu: Click 'Tải CSV' để download")

# Footer
st.divider()
st.markdown("""<div style='opacity:.7;font-size:14px;text-align:center'>📊 Forecast Explorer • Powered by LightGBM + MLflow + Delta Lake</div>""", unsafe_allow_html=True)
