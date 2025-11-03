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

# Handle example selection
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
        
        # Add assistant response to history
        answer = result.get("answer", "Không có câu trả lời")
        st.session_state.chat_history.append(("assistant", answer))
        
        # Display assistant message
        with st.chat_message("assistant"):
            st.markdown(answer)

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

