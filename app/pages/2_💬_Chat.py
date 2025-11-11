"""
Chat Page - SQL + RAG Chatbot
Interactive chat interface for querying the data lakehouse
"""
import streamlit as st
import requests
import uuid
import pandas as pd
import os
from datetime import datetime


# ============================================================================
# Configuration
# ============================================================================

def get_chat_service_url():
    """Get Chat Service URL from secrets, env, or fallback"""
    try:
        return st.secrets.get("CHAT_SERVICE_URL")
    except:
        return os.getenv("CHAT_SERVICE_URL", "http://chat_service:8001")

CHAT_SERVICE_URL = get_chat_service_url()

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
.badge{display:inline-flex;gap:6px;align-items:center;padding:4px 10px;border-radius:999px;
  background:rgba(34,211,238,.12);border:1px solid rgba(34,211,238,.25);font-size:12px}
.muted{color:var(--muted)}

.stButton>button{border-radius:14px;padding:.8rem 1.1rem;font-weight:700;font-size:15px}
.section-title{display:flex;align-items:center;gap:10px;margin:1.5rem 0 1rem}
.section-title h3{margin:0}
.section-title:after{content:"";flex:1;height:1px;background:linear-gradient(90deg,transparent, var(--line))}
</style>
""", unsafe_allow_html=True)

# ============================================================================
# Session State
# ============================================================================

if "session_id" not in st.session_state:
    st.session_state.session_id = uuid.uuid4().hex[:16]

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "last_sql" not in st.session_state:
    st.session_state.last_sql = None

if "last_preview" not in st.session_state:
    st.session_state.last_preview = None

if "last_citations" not in st.session_state:
    st.session_state.last_citations = None

if "execution_time" not in st.session_state:
    st.session_state.execution_time = None

if "last_suggestions" not in st.session_state:
    st.session_state.last_suggestions = None



# ============================================================================
# Helper Functions
# ============================================================================

def get_example_questions():
    """Get example questions from API"""
    try:
        response = requests.get(f"{CHAT_SERVICE_URL}/examples", timeout=5)
        if response.ok:
            return response.json().get("examples", [])
    except:
        pass
    
    # Fallback examples
    return [
        "Doanh thu theo tháng gần đây?",
        "Top 10 sản phẩm bán chạy nhất?",
        "Phân bố đơn hàng theo vùng miền?",
        "Phương thức thanh toán nào phổ biến nhất?",
    ]


def render_prompt_chip(label: str, prompt: str, key: str):
    """Render a prompt chip button - auto-submit when clicked"""
    if st.button(label, key=key, use_container_width=True):
        st.session_state.selected_example = prompt
        st.rerun()


def get_prompt_chips():
    """Define all prompt chips organized by category"""
    return {
        "Tổng quan & Tăng trưởng": [
            ("Tổng quan theo tháng", "Tổng quan doanh thu 2017-01-01 → 2018-01-01: GMV, Orders, Units, AOV theo tháng."),
            ("MoM/YoY 2017", "Tính MoM và YoY GMV theo tháng giai đoạn 2017-01-01 → 2018-01-01."),
            ("Top danh mục (Pareto)", "Top 15 danh mục đóng góp GMV giai đoạn 2017-01-01 → 2018-01-01 (bảng + Pareto%)."),
        ],
        "Danh mục & Sản phẩm": [
            ("Top SP theo GMV (kèm info)", "Top 20 sản phẩm GMV trong danh mục computers năm 2017, kèm thông tin sản phẩm."),
            ("Top SP theo số đơn", "Top 20 sản phẩm theo số đơn năm 2017, kèm category_en & kích thước."),
            ("Heatmap Category×Month", "GMV theo danh mục × tháng trong năm 2017 (heatmap)."),
            ("AOV theo danh mục", "Giá trị trung bình đơn hàng (AOV) theo danh mục năm 2017."),
        ],
        "Địa lý (Geography)": [
            ("GMV theo bang", "GMV theo bang (state) năm 2017, sắp xếp giảm dần, hiển thị Top 10."),
            ("GMV theo thành phố", "GMV theo thành phố (city) trong bang SP năm 2017, Top 10."),
            ("Tăng trưởng theo bang", "Tốc độ tăng trưởng GMV theo bang 2017-01-01 → 2018-01-01."),
        ],
        "Người bán (Seller)": [
            ("Top seller theo GMV", "Top 10 seller theo GMV năm 2017: GMV, Orders, Units, on_time_rate, cancel_rate, avg_review_score."),
            ("Seller có vấn đề SLA", "Seller có on_time_rate < 90% nhưng GMV > 20000 trong năm 2017."),
            ("Trend review score", "Trend review_score theo tháng của seller trong năm 2017."),
        ],
        "Vận hành (SLA/Delivery)": [
            ("On-time rate theo tháng", "On-time rate và Avg delivery days theo tháng năm 2017 (exclude canceled)."),
            ("Top bang delivery chậm", "Top 10 bang có Avg delivery days cao nhất trong năm 2017."),
            ("SLA theo danh mục", "Danh mục computers: on_time_rate theo tháng năm 2017."),
        ],
        "Thanh toán (Finance)": [
            ("Payment mix theo tháng", "Payment mix theo tháng năm 2017 (tỷ trọng theo payment_value)."),
            ("Tỷ lệ trả góp", "Tỷ lệ dùng installments theo tháng năm 2017."),
        ],
        "Khách hàng (Cohort/Retention)": [
            ("Cohort heatmap", "Cohort từ 2017-01 → 2017-06: retention heatmap (0–12 tháng)."),
            ("Retention sau k tháng", "Retention sau 3 tháng của cohort 2017-01 là bao nhiêu? Kèm bảng chi tiết."),
            ("Orders theo cohort", "Orders theo thời gian của mỗi cohort (line chart) giai đoạn 2017-01-01 → 2018-01-01."),
        ],
        "Dự báo (Forecast)": [
            ("Dự báo GMV 28 ngày", "Dự báo GMV 28 ngày tới cho computers (actual vs forecast + dải tin cậy)."),
            ("Sai số dự báo", "Sai số dự báo MAE/MAPE theo tháng trong năm 2017."),
        ],
    }


def send_question(question: str):
    """Send question to chat service"""
    try:
        response = requests.post(
            f"{CHAT_SERVICE_URL}/ask",
            json={
                "session_id": st.session_state.session_id,
                "question": question,
                "prefer_sql": True
            },
            timeout=60
        )
        
        if response.ok:
            return response.json()
        else:
            st.error(f"❌ Lỗi API: {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        st.error("⏰ Timeout: Query took too long to execute")
        return None
    except Exception as e:
        st.error(f"❌ Lỗi kết nối: {str(e)}")
        return None


# ============================================================================
# UI
# ============================================================================

st.title("💬 Chat với Dữ liệu")
st.caption("Hỏi đáp dữ liệu Brazilian E-commerce bằng ngôn ngữ tự nhiên (SQL + RAG)")

# Info banner
st.info("""
⚙️ Truy vấn **read-only** trên schema **gold/platinum**; mặc định áp **LIMIT** và **timeout**. 
Thời gian lọc dùng chuẩn half-open `[start, end_next)` để tránh lỗi biên. `category_en` & `state` là cột chuẩn hoá.

🧩 Khi kết quả có **product_id**, hệ thống **tự enrich** thông tin sản phẩm (category_en, kích thước/khối lượng). 
Gõ 'kèm thông tin sản phẩm' để buộc join đầy đủ.
""")

# Sidebar - Examples & Info
with st.sidebar:
    st.header("📚 Câu hỏi mẫu")
    
    examples = get_example_questions()
    
    for example in examples:
        if st.button(example, key=f"ex_{hash(example)}", use_container_width=True):
            st.session_state.selected_example = example
    
    st.divider()
    
    st.header("ℹ️ Thông tin")
    st.info("""
    **Hệ thống có thể trả lời:**
    - 📊 Truy vấn dữ liệu (SQL)
    - 📚 Tài liệu tham khảo (RAG)
    - 💡 Gợi ý phân tích
    
    **An toàn:**
    - ✅ Chỉ cho phép SELECT (read-only)
    - ✅ Giới hạn số dòng trả về
    - ✅ Timeout tự động
    - ✅ Log đầy đủ
    """)
    
    if st.button("🔄 Reset Chat", use_container_width=True):
        st.session_state.chat_history = []
        st.session_state.session_id = uuid.uuid4().hex[:16]
        st.session_state.last_sql = None
        st.session_state.last_preview = None
        st.session_state.last_citations = None
        st.session_state.execution_time = None
        st.rerun()

# Prompt Chips Section
st.markdown("### 🧠 Gợi ý câu hỏi nhanh")
prompt_chips = get_prompt_chips()

# Render chips in expanders
for category, chips in prompt_chips.items():
    with st.expander(category, expanded=(category == "Tổng quan & Tăng trưởng")):
        cols = st.columns(3)
        for i, (label, prompt) in enumerate(chips):
            col_idx = i % 3
            with cols[col_idx]:
                render_prompt_chip(label, prompt, f"chip_{category}_{i}")

st.caption("💡 Click vào chip để tự động điền prompt vào ô chat. Bạn có thể chỉnh sửa các tham số (năm, Top-N, category...) trước khi gửi.")

# Main chat interface
st.divider()

# Welcome message if no history
if not st.session_state.chat_history:
    st.info("""
    👋 **Xin chào! Tôi là trợ lý phân tích dữ liệu Brazilian E-commerce.**
    
    💡 Bạn có thể hỏi tôi về:
    - Doanh thu, sản phẩm, đơn hàng
    - Phân tích theo khu vực, danh mục
    - Phương thức thanh toán, đánh giá khách hàng
    
    📌 Chọn câu hỏi mẫu bên trái hoặc nhập câu hỏi của bạn!
    """)
    
    # Show suggestion cards
    st.markdown("### 🎯 Câu hỏi phổ biến:")
    cols = st.columns(2)
    examples = get_example_questions()
    for i, example in enumerate(examples[:6]):
        col_idx = i % 2
        with cols[col_idx]:
            if st.button(f"💬 {example}", key=f"welcome_{i}", use_container_width=True):
                st.session_state.selected_example = example
                st.rerun()

# Display chat history
for role, message in st.session_state.chat_history:
    with st.chat_message(role):
        st.markdown(message)
        
        # If assistant message contains suggestions, show as buttons
        if role == "assistant" and "Gợi ý câu hỏi phổ biến:" in message:
            st.markdown("---")
            st.markdown("**👇 Click để hỏi:**")
            
            # Extract suggestions from message
            examples = get_example_questions()
            cols = st.columns(2)
            for i, example in enumerate(examples[:6]):
                col_idx = i % 2
                with cols[col_idx]:
                    if st.button(example, key=f"suggest_{hash(message)}_{i}", use_container_width=True):
                        st.session_state.selected_example = example
                        st.rerun()

# Chat input
question = st.chat_input("Nhập câu hỏi của bạn...", key="chat_input")

# Handle example selection (from sidebar or prompt chips)
if "selected_example" in st.session_state:
    question = st.session_state.selected_example
    del st.session_state.selected_example

# Process question
if question:
    # Add user message to history
    st.session_state.chat_history.append(("user", question))
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(question)
    
    # Send to API
    with st.spinner("🤔 Đang suy nghĩ..."):
        result = send_question(question)
    
    if result:
        # Store results
        st.session_state.last_sql = result.get("sql")
        st.session_state.last_preview = result.get("rows_preview")
        st.session_state.last_citations = result.get("citations")
        st.session_state.execution_time = result.get("execution_time_ms")
        st.session_state.last_suggestions = result.get("suggestions")  # Store suggestions
        
        # Add assistant response to history
        answer = result.get("answer", "Không có câu trả lời")
        st.session_state.chat_history.append(("assistant", answer))
        
        # Display assistant message
        with st.chat_message("assistant"):
            st.markdown(answer)
            
            # Display suggestions as clickable buttons
            suggestions = result.get("suggestions")
            if suggestions and len(suggestions) > 0:
                st.markdown("---")
                st.markdown("**💡 Gợi ý câu hỏi tiếp theo:**")
                
                # Display suggestions in columns
                cols = st.columns(min(len(suggestions), 3))
                for i, sugg in enumerate(suggestions[:3]):
                    col_idx = i % 3
                    with cols[col_idx]:
                        if st.button(
                            sugg, 
                            key=f"sugg_{hash(question)}_{i}",
                            use_container_width=True
                        ):
                            # Set selected suggestion as next question
                            st.session_state.selected_example = sugg
                            st.rerun()

# Display SQL & Results
if st.session_state.last_sql or st.session_state.last_preview or st.session_state.last_citations:
    st.divider()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.session_state.execution_time:
            st.metric("⏱️ Thời gian thực thi", f"{st.session_state.execution_time}ms")
    
    with col2:
        if st.session_state.last_preview:
            st.metric("📊 Số dòng", len(st.session_state.last_preview))
    
    with col3:
        if st.session_state.last_citations:
            st.metric("📚 Tài liệu tham khảo", len(st.session_state.last_citations))
    
    # SQL Query
    if st.session_state.last_sql:
        with st.expander("🔍 SQL Query", expanded=False):
            st.code(st.session_state.last_sql, language="sql")
            
            # Copy button
            if st.button("📋 Copy SQL"):
                st.toast("SQL copied to clipboard!")
    
    # Data Preview
    if st.session_state.last_preview:
        with st.expander("📊 Kết quả (Preview 50 dòng đầu)", expanded=True):
            df = pd.DataFrame(st.session_state.last_preview)
            
            # Reorder columns if product_id present (prioritize product info)
            if "product_id" in df.columns:
                cols = df.columns.tolist()
                priority = [c for c in ["product_id", "category_en", "orders", "units", "gmv", "aov", 
                                       "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"] 
                           if c in cols]
                others = [c for c in cols if c not in priority]
                df = df[priority + others]
                
                # Show info message about product enrichment
                st.info("💡 Đã tự động bổ sung **thông tin sản phẩm** từ `gold.dim_product` và `gold.dim_product_category` (cột: `category_en`, kích thước/khối lượng).")
            
            # Display dataframe
            st.dataframe(
                df,
                use_container_width=True,
                height=400
            )
            
            # Export buttons
            col1, col2 = st.columns(2)
            
            with col1:
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
            
            with col2:
                # Basic statistics
                if st.button("📈 Show Statistics"):
                    st.write(df.describe())
    
    # Citations
    if st.session_state.last_citations:
        with st.expander("📚 Tài liệu tham khảo", expanded=False):
            for i, citation in enumerate(st.session_state.last_citations, 1):
                st.markdown(f"""
                **{i}. {citation.get('source', 'Unknown')}**  
                Độ liên quan: `{citation.get('score', 0):.2f}`
                
                > {citation.get('text', 'No preview available')[:300]}...
                """)
                st.divider()

# Footer
st.divider()
st.caption(f"Session ID: `{st.session_state.session_id}` | Service: `{CHAT_SERVICE_URL}`")

