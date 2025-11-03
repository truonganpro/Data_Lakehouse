"""
Forecast Explorer - ML Forecasting Showcase
Interactive interface for exploring demand forecasts
"""
import streamlit as st
import pandas as pd
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

# Enhanced CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

:root{
  --bg:#0b1220; --bg2:#10192b; --card:#0f172a; --line:#1f2a44;
  --text:#e2e8f0; --muted:#94a3b8; --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; --pri:#22d3ee;
}

html, body, [class*=css] { font-family: 'Inter', sans-serif; }
.main .block-container{max-width:1280px;padding-top:1rem;padding-bottom:3rem}
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
        st.error(f"Lỗi kết nối Trino: {e}")
        return pd.DataFrame()

# ====== UI ======
st.title("📈 Forecast Explorer")
st.caption("Khám phá dự báo nhu cầu sản phẩm với Machine Learning")

# Info Section
info_col1, info_col2, info_col3 = st.columns(3)

with info_col1:
    st.markdown("""
    <div class='metric-card'>
    <div style='font-size:14px;opacity:.8'>Model</div>
    <div style='font-size:20px;font-weight:700;margin-top:4px'>LightGBM</div>
    <div style='opacity:.6'>Gradient Boosting</div>
    </div>
    """, unsafe_allow_html=True)

with info_col2:
    st.markdown("""
    <div class='metric-card'>
    <div style='font-size:14px;opacity:.8'>Forecast Horizon</div>
    <div style='font-size:20px;font-weight:700;margin-top:4px'>28 Ngày</div>
    <div style='opacity:.6'>Recursive Roll-forward</div>
    </div>
    """, unsafe_allow_html=True)

with info_col3:
    st.markdown("""
    <div class='metric-card'>
    <div style='font-size:14px;opacity:.8'>Features</div>
    <div style='font-size:20px;font-weight:700;margin-top:4px'>15+</div>
    <div style='opacity:.6'>Lags, Rolling, Calendar</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Main Content Tabs
tab_forecast, tab_monitoring, tab_mlflow = st.tabs([
    "🔮 Dự báo", 
    "📊 Monitoring", 
    "🧪 MLflow"
])

with tab_forecast:
    st.markdown("### 🔮 Dữ liệu dự báo")
    
    st.info("""
    **Lưu ý**: Dataset Brazilian E-commerce từ 2016-2018 (dữ liệu lịch sử). 
    
    Để xem forecasts thực tế, chạy forecast pipeline qua Dagster:
    - Mở Dagster UI
    - Jobs → `forecast_job`
    - Launch Run
    
    Kết quả sẽ được lưu vào `platinum.demand_forecast`
    """)
    
    # Check if table exists
    try:
        check_sql = """
        SELECT COUNT(*) as count
        FROM platinum.demand_forecast
        LIMIT 1
        """
        check_df = run_query(check_sql, "platinum")
        
        if not check_df.empty and check_df['count'][0] > 0:
            # Load forecast sample
            forecast_sql = """
            SELECT 
                forecast_date,
                product_id,
                region_id,
                horizon,
                yhat,
                yhat_lo,
                yhat_hi,
                model_name
            FROM platinum.demand_forecast
            WHERE horizon <= 7
            ORDER BY forecast_date DESC, product_id, horizon
            LIMIT 100
            """
            
            forecast_df = run_query(forecast_sql, "platinum")
            
            if not forecast_df.empty:
                st.success(f"✅ Đã tải {len(forecast_df):,} dòng dự báo")
                
                # Display data
                st.dataframe(forecast_df, use_container_width=True, height=400)
                
                # Download
                csv = forecast_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Tải CSV",
                    csv,
                    f"demand_forecast_{datetime.now().strftime('%Y%m%d')}.csv",
                    "text/csv",
                    use_container_width=True
                )
            else:
                st.warning("Không có dữ liệu forecast. Chạy forecast_job để tạo dự báo.")
        else:
            st.warning("Bảng `platinum.demand_forecast` chưa có dữ liệu. Chạy forecast_job trước.")
    except Exception as e:
        st.warning(f"Chưa thể truy cập bảng forecast: {e}")
    
    # CTA to Dagster
    st.markdown("### 🚀 Chạy Forecast Pipeline")
    st.link_button("⚙️ Mở Dagster → forecast_job", "http://localhost:3001", use_container_width=True)

with tab_monitoring:
    st.markdown("### 📊 Forecast Monitoring")
    
    st.info("""
    **Monitoring Metrics**:
    - sMAPE: Symmetric Mean Absolute Percentage Error
    - MAE: Mean Absolute Error  
    - RMSE: Root Mean Square Error
    
    So sánh actual vs forecast để đánh giá độ chính xác.
    """)
    
    # Check if monitoring table exists
    try:
        mon_check_sql = """
        SELECT COUNT(*) as count
        FROM platinum.forecast_monitoring
        LIMIT 1
        """
        mon_check_df = run_query(mon_check_sql, "platinum")
        
        if not mon_check_df.empty and mon_check_df['count'][0] > 0:
            # Load monitoring data
            mon_sql = """
            SELECT 
                date,
                product_id,
                region_id,
                y_actual,
                yhat,
                abs_error,
                pct_error,
                smape,
                model_name
            FROM platinum.forecast_monitoring
            ORDER BY date DESC
            LIMIT 100
            """
            
            mon_df = run_query(mon_sql, "platinum")
            
            if not mon_df.empty:
                st.success(f"✅ Đã tải {len(mon_df):,} dòng monitoring")
                
                # Metrics
                avg_smape = mon_df['smape'].mean() if 'smape' in mon_df.columns else 0
                avg_mae = mon_df['abs_error'].mean() if 'abs_error' in mon_df.columns else 0
                
                m1, m2, m3 = st.columns(3)
                m1.metric("Avg sMAPE", f"{avg_smape:.2f}%")
                m2.metric("Avg MAE", f"{avg_mae:.2f}")
                m3.metric("Records", f"{len(mon_df):,}")
                
                # Display data
                st.dataframe(mon_df, use_container_width=True, height=400)
            else:
                st.warning("Không có dữ liệu monitoring.")
        else:
            st.warning("Bảng `platinum.forecast_monitoring` chưa có dữ liệu.")
    except Exception as e:
        st.warning(f"Chưa thể truy cập bảng monitoring: {e}")

with tab_mlflow:
    st.markdown("### 🧪 MLflow Experiment Tracking")
    
    st.markdown("""
    **MLflow** được sử dụng để:
    - Track experiments & hyperparameters
    - Log model metrics (RMSE, MAE, sMAPE, R2)
    - Store model artifacts
    - Version models
    
    **Model Registry**:
    - Model name: `lightgbm_global_recursive`
    - Storage: MinIO (S3-compatible)
    - Tracking DB: MySQL
    """)
    
    st.markdown("### 📦 Model Details")
    
    detail_col1, detail_col2 = st.columns(2)
    
    with detail_col1:
        st.markdown("""
        **Features**:
        - Lagged values (1, 7, 28 days)
        - Rolling averages (7, 28 days)
        - Calendar features (dow, month, is_weekend)
        - Product & region embeddings
        """)
    
    with detail_col2:
        st.markdown("""
        **Hyperparameters**:
        - `num_leaves`: 31
        - `learning_rate`: 0.05
        - `n_estimators`: 100
        - `max_depth`: -1 (unlimited)
        """)
    
    st.info("💡 Tip: Mở MLflow UI để xem chi tiết experiments (nếu đã setup)")

# Footer
st.divider()
st.markdown("""
<div style='opacity:.7;font-size:14px;text-align:center'>
📊 Forecast Explorer • Powered by LightGBM + MLflow + Delta Lake
</div>
""", unsafe_allow_html=True)

