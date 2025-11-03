import streamlit as st
import requests
import time
import os

# Set page config
st.set_page_config(
    page_title="Lakehouse Dashboard",
    page_icon="🪩",
    layout="wide"
)

# Enhanced CSS with animations and polish
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
.badge{display:inline-flex;gap:6px;align-items:center;padding:4px 10px;border-radius:999px;
  background:rgba(34,211,238,.12);border:1px solid rgba(34,211,238,.25);font-size:12px;text-decoration:none;color:var(--text)}
.badge:hover{background:rgba(34,211,238,.18)}
.kpi{font-size:28px;font-weight:700}
.muted{color:var(--muted)}

.stButton>button, .stDownloadButton>button { border-radius: 14px; padding: .8rem 1.1rem; font-weight: 700; font-size: 15px; }

.status-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:1rem 0}
.status .name{font-weight:600;font-size:16px}
.status .url{font-size:12px;color:var(--muted);margin-top:4px}
.status .state{margin-top:12px;font-size:13px}
.pulse{position:relative;padding-left:22px}
.pulse:before{content:"";position:absolute;left:0;top:50%;width:10px;height:10px;border-radius:50%;
  transform:translateY(-50%);background:var(--err);box-shadow:0 0 0 0 rgba(239,68,68,.6);animation:pulse 1.6s infinite}
.ok .pulse:before{background:var(--ok);box-shadow:0 0 0 0 rgba(34,197,94,.5);animation:pulse-ok 1.6s infinite}
@keyframes pulse { 0%{box-shadow:0 0 0 0 rgba(239,68,68,.6)} 70%{box-shadow:0 0 0 12px rgba(239,68,68,0)} 100%{box-shadow:0 0 0 0 rgba(239,68,68,0)} }
@keyframes pulse-ok { 0%{box-shadow:0 0 0 0 rgba(34,197,94,.5)} 70%{box-shadow:0 0 0 12px rgba(34,197,94,0)} 100%{box-shadow:0 0 0 0 rgba(34,197,94,0)} }

.section-title{display:flex;align-items:center;gap:10px;margin:1.5rem 0 1rem}
.section-title h3{margin:0}
.section-title:after{content:"";flex:1;height:1px;background:linear-gradient(90deg,transparent, var(--line))}

.quick-dock{position:fixed;right:18px;bottom:18px;display:flex;gap:8px;z-index:999}
.quick-dock a{background:var(--card);border:1px solid var(--line);padding:10px 12px;border-radius:12px;text-decoration:none;color:var(--text);font-size:13px;font-weight:600;transition:all 0.2s}
.quick-dock a:hover{border-color:#294166;background:var(--bg2)}

.service-card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px;margin-bottom:12px;transition:all 0.2s}
.service-card:hover{border-color:#294166;transform:translateY(-2px)}
.service-card .title{font-weight:700;font-size:18px;margin-bottom:6px}
.service-card .desc{color:var(--muted);font-size:14px;margin-bottom:12px}

.hero-section{margin-bottom:2rem}
.hero-section h1{margin-bottom:0.3rem}
.hero-section .subtitle{color:var(--muted);font-size:16px;margin-bottom:1.5rem}

.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] { padding:10px 14px;border-radius:10px;background:var(--card);border:1px solid var(--line) }
.stTabs [data-baseweb="tab"][aria-selected="true"] { background:var(--bg2);border-color:var(--pri) }

a{text-decoration:none!important}
</style>
""", unsafe_allow_html=True)

# Hero Section
st.markdown("""
<div class="hero-section">
<h1>🪩 Data Lakehouse – Modern Data Stack</h1>
<div class="subtitle"><strong>ETL • Forecast • OLAP • AI Chat • BI</strong> — tất cả trong một Control Center</div>
</div>
""", unsafe_allow_html=True)

# Quick CTA Buttons
ctaA, ctaB, ctaC = st.columns([1,1,1])
with ctaA:
    st.page_link("pages/1_📊_Query_Window.py", label="📊 Query Window", help="OLAP ROLLUP/GROUPING SETS", use_container_width=True)
with ctaB:
    st.page_link("pages/2_💬_Chat.py", label="💬 Chat Analytics", help="Natural Language to SQL", use_container_width=True)
with ctaC:
    st.link_button("⚙️ Dagster", "http://localhost:3001", help="ETL Orchestration", use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# Service Status with Pulse Animation
# Use internal Docker network URLs for health checks
SERVICES = [
    ("Trino", "http://trino:8080", "Port 8082 (ext)"),
    ("Dagster", "http://de_dagster_dagit:3001", "Port 3001"),
    ("Metabase", "http://metabase:3000", "Port 3000"),
    ("MinIO", "http://minio:9000", "Port 9001 (console)"),
]

st.markdown("### 🔎 Trạng thái hệ thống")

# Build status cards with ping - using Streamlit native components
status_cols = st.columns(4)

for idx, (name, url, port) in enumerate(SERVICES):
    t0 = time.time()
    ok = False
    try:
        requests.get(url, timeout=1.2)
        ok = True
    except:
        pass
    latency = int((time.time() - t0) * 1000)
    
    with status_cols[idx]:
        # Display in card-like container
        with st.container():
            st.markdown(f"**{name}**")
            st.caption(port)
            
            if ok:
                st.success(f"✅ Online • {latency}ms", icon="🟢")
            else:
                st.error(f"⛔ Offline • {latency}ms", icon="🔴")
            
            # External URL button
            external_url = url.replace("trino:8080", "localhost:8082").replace("de_dagster_dagit:3001", "localhost:3001").replace("metabase:3000", "localhost:3000").replace("minio:9000", "localhost:9001")
            st.link_button(f"Mở {name}", external_url, use_container_width=True, type="secondary")

st.divider()

# Tabs for Services & Architecture
tab_services, tab_arch, tab_docs = st.tabs(["🧩 Dịch vụ", "🗺️ Kiến trúc", "📚 Tài liệu"])

with tab_services:
    sc1, sc2 = st.columns(2)
    
    with sc1:
        st.markdown("<div class='section-title'><h3>📊 Phân tích dữ liệu</h3></div>", unsafe_allow_html=True)
        
        # Query Window
        st.markdown("""
        <div class='service-card'>
        <div class='title'>🧮 Query Window</div>
        <div class='desc'>Truy vấn đa chiều (OLAP) với ROLLUP, GROUPING SETS</div>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/1_📊_Query_Window.py", label="Mở Query Window", icon="📊", use_container_width=True)
        
        st.markdown("")
        
        # Chat
        st.markdown("""
        <div class='service-card'>
        <div class='title'>💬 Chat Analytics</div>
        <div class='desc'>Hỏi đáp bằng ngôn ngữ tự nhiên với SQL + RAG</div>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/2_💬_Chat.py", label="Mở Chat", icon="💬", use_container_width=True)
        
        st.markdown("")
        
        # Forecast Explorer
        st.markdown("""
        <div class='service-card'>
        <div class='title'>📈 Forecast Explorer</div>
        <div class='desc'>Khám phá dự báo nhu cầu với Machine Learning</div>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/3_📈_Forecast_Explorer.py", label="Mở Forecast Explorer", icon="📈", use_container_width=True)
        
        st.markdown("")
        
        # Metabase
        st.markdown("""
        <div class='service-card'>
        <div class='title'>📊 Metabase</div>
        <div class='desc'>BI dashboards cho business users</div>
        </div>
        """, unsafe_allow_html=True)
        st.link_button("Mở Metabase", "http://localhost:3000", use_container_width=True)
    
    with sc2:
        st.markdown("<div class='section-title'><h3>🛠️ Vận hành & Quản lý</h3></div>", unsafe_allow_html=True)
        
        # Dagster
        st.markdown("""
        <div class='service-card'>
        <div class='title'>⚙️ Dagster</div>
        <div class='desc'>Orchestration & monitoring cho ETL pipelines</div>
        </div>
        """, unsafe_allow_html=True)
        st.link_button("Mở Dagster", "http://localhost:3001", use_container_width=True)
        
        st.markdown("")
        
        # MinIO
        st.markdown("""
        <div class='service-card'>
        <div class='title'>🪣 MinIO Console</div>
        <div class='desc'>Quản lý object storage (S3-compatible)</div>
        </div>
        """, unsafe_allow_html=True)
        st.link_button("Mở MinIO", "http://localhost:9001", use_container_width=True)
        
        st.markdown("")
        
        # Spark
        st.markdown("""
        <div class='service-card'>
        <div class='title'>🔥 Spark Master</div>
        <div class='desc'>Monitor Spark cluster & jobs</div>
        </div>
        """, unsafe_allow_html=True)
        st.link_button("Mở Spark UI", "http://localhost:8080", use_container_width=True)
        
        st.markdown("")
        
        # Trino
        st.markdown("""
        <div class='service-card'>
        <div class='title'>🔺 Trino UI</div>
        <div class='desc'>Query coordinator & cluster info</div>
        </div>
        """, unsafe_allow_html=True)
        st.link_button("Mở Trino UI", "http://localhost:8082", use_container_width=True)

with tab_arch:
    st.markdown("### 🏗️ Kiến trúc hệ thống")
    
    st.code("""
┌─────────────────────────────────────────────────────────────────┐
│                       USER INTERFACES                            │
├──────────────┬──────────────┬──────────────┬───────────────────┤
│  Streamlit   │  Metabase    │   Dagster    │   Jupyter         │
│  (Port 8501) │  (Port 3000) │  (Port 3001) │   (Port 8888)     │
└──────────────┴──────────────┴──────────────┴───────────────────┘
        ↓              ↓              ↓                ↓
┌─────────────────────────────────────────────────────────────────┐
│                  QUERY & PROCESSING LAYER                        │
├──────────────┬──────────────┬──────────────┬───────────────────┤
│    Trino     │    Spark     │   MLflow     │   Chat Service    │
│  (Port 8082) │  (Port 8080) │  (Port 5000) │   (Port 8001)     │
└──────────────┴──────────────┴──────────────┴───────────────────┘
        ↓              ↓              ↓                ↓
┌─────────────────────────────────────────────────────────────────┐
│                  STORAGE & METADATA LAYER                        │
├──────────────┬──────────────┬──────────────┬───────────────────┤
│  Delta Lake  │     MinIO    │    MySQL     │   Qdrant          │
│  (Lakehouse) │  (S3 Object) │ (Metadata)   │   (Vector DB)     │
└──────────────┴──────────────┴──────────────┴───────────────────┘
    """, language="text")
    
    st.markdown("### 📊 Medallion Architecture")
    
    arch_col1, arch_col2 = st.columns(2)
    
    with arch_col1:
        st.markdown("""
        **Data Flow:**
        
        ```
        MySQL (Source)
            ↓
        Bronze Layer (Raw)
            ↓
        Silver Layer (Cleaned)
            ↓
        Gold Layer (Star Schema)
            ↓
        Platinum Layer (Datamarts)
        ```
        """)
    
    with arch_col2:
        st.markdown("""
        **Statistics:**
        
        - **Bronze**: 9 tables (raw data)
        - **Silver**: 11 tables (cleaned)
        - **Gold**: 10 tables (star schema)
        - **Platinum**: 8 tables (datamarts)
        - **Total**: ~500MB storage
        """)

with tab_docs:
    st.markdown("### 📚 Tài liệu dự án")
    
    doc_col1, doc_col2 = st.columns(2)
    
    with doc_col1:
        st.markdown("""
        **Tài liệu kỹ thuật:**
        
        - `PROJECT_OVERVIEW.md` - Tổng quan hoàn chỉnh
        - `FORECAST_FILES.txt` - ML & Forecasting system
        - `STREAMLIT_APP_FILES.txt` - UI application
        - `UI_UX_IMPROVEMENTS.md` - UI/UX changelog
        - `README.md` - Quick start guide
        """)
    
    with doc_col2:
        st.markdown("""
        **Use Cases chính:**
        
        1. **Business Analytics** - Truy vấn OLAP
        2. **Demand Forecasting** - Dự báo 28 ngày
        3. **Natural Language Queries** - Chat interface
        4. **BI Dashboards** - Metabase reports
        """)
    
    st.info("💡 Xem thêm chi tiết trong các file tài liệu tại thư mục gốc dự án")

# Quick Actions Dock
st.markdown("""
<div class='quick-dock'>
  <a href='http://localhost:3001' target='_blank' title='Dagster'>⚙️</a>
  <a href='http://localhost:3000' target='_blank' title='Metabase'>📊</a>
  <a href='http://localhost:9001' target='_blank' title='MinIO'>🪣</a>
  <a href='http://localhost:8080' target='_blank' title='Spark'>🔥</a>
</div>
""", unsafe_allow_html=True)

# Footer
st.divider()
st.markdown("""
<div style='display:flex;justify-content:space-between;opacity:.8;font-size:14px'>
  <span>🪩 Data Lakehouse • Modern Data Stack</span>
  <span>Built with ❤️ by <b>Truong An</b> • MIT License</span>
</div>
""", unsafe_allow_html=True)

st.caption("💡 Mẹo: Dùng **Quick Dock** (góc phải dưới) hoặc sidebar để di chuyển nhanh giữa các trang")
