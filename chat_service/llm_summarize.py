# -*- coding: utf-8 -*-
"""
LLM-based result summarization module using Google Gemini
"""
import os
from typing import List, Dict, Optional
import google.generativeai as genai


PROMPT_SUMMARY = """Bạn là nhà phân tích dữ liệu chuyên nghiệp. Tóm tắt kết quả truy vấn NGẮN GỌN (2-4 câu) bằng tiếng Việt, có nêu số liệu nổi bật.

ĐẦU VÀO:
- Câu hỏi: {question}
- Bảng dữ liệu (tối đa 50 hàng): 
{table_preview}

{citations_section}

YÊU CẦU:
1. **Phạm vi**: Nêu 1 câu về phạm vi dữ liệu (tháng/quý, top-N nếu có).
2. **Xu hướng**: 1-2 câu về xu hướng ↑↓ (tăng/giảm, cao nhất/thấp nhất).
3. **Điểm đáng chú ý**: 1 câu nêu điều đáng chú ý (outlier, tăng/giảm mạnh, top/bottom).
4. **Không bịa số**: Chỉ dùng số liệu có trong bảng.
5. **Ngắn gọn**: Tối đa 4 câu, không liệt kê quá dài.

Ví dụ:
- "Doanh thu theo tháng từ 06-08/2018, giảm dần từ 1.23M → 987K. Tháng cao nhất là 07/2018 với 1.12M. Xu hướng giảm nhẹ nhưng ổn định."
- "Top 10 sản phẩm bán chạy, GMV từ 50K → 200K. Sản phẩm số 1 có GMV 200K, chiếm 15% tổng. Phân bố đều, không có outlier."

Trả lời NGẮN GỌN, CHÍNH XÁC, DỄ HIỂU.
"""


def summarize_with_gemini(
    question: str,
    table_preview: List[Dict],
    citations: Optional[List[Dict]] = None
) -> Optional[str]:
    """
    Summarize query results using Gemini
    
    Args:
        question: Original user question
        table_preview: List of dictionaries (rows from SQL result)
        citations: Optional list of RAG citations
        
    Returns:
        Summary text or None if LLM_PROVIDER is not gemini
    """
    if os.getenv("LLM_PROVIDER", "none").lower() != "gemini":
        return None
    
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("⚠️  GOOGLE_API_KEY not set, cannot use Gemini for summarization")
        return None
    
    try:
        genai.configure(api_key=api_key)
        
        # Use gemini-1.5-flash for fast, cost-effective summarization
        model = genai.GenerativeModel("gemini-1.5-flash")
        
        # Format table preview
        if table_preview:
            # Limit to first 50 rows
            preview_rows = table_preview[:50]
            
            # Format as text table
            if preview_rows:
                # Get column names from first row
                columns = list(preview_rows[0].keys())
                
                table_text = "| " + " | ".join(columns) + " |\n"
                table_text += "|" + "|".join(["---"] * len(columns)) + "|\n"
                
                for row in preview_rows[:10]:  # Show first 10 rows in detail
                    values = [str(row.get(col, "")) for col in columns]
                    table_text += "| " + " | ".join(values) + " |\n"
                
                if len(preview_rows) > 10:
                    table_text += f"\n... và {len(preview_rows) - 10} dòng khác.\n"
                    table_text += f"\nTổng cộng: {len(table_preview)} dòng."
            else:
                table_text = "(Không có dữ liệu)"
        else:
            table_text = "(Không có dữ liệu)"
        
        # Format citations
        citations_section = ""
        if citations and len(citations) > 0:
            citations_section = "- Tài liệu tham khảo:\n"
            for cite in citations[:3]:  # Show top 3 citations
                citations_section += f"  * {cite.get('source', 'unknown')} (độ liên quan: {cite.get('score', 0):.2f})\n"
        
        prompt = PROMPT_SUMMARY.format(
            question=question,
            table_preview=table_text,
            citations_section=citations_section
        )
        
        print(f"🤖 Summarizing with Gemini...")
        
        response = model.generate_content(prompt)
        summary = response.text.strip()
        
        print(f"✅ Generated summary: {summary[:100]}...")
        
        return summary
        
    except Exception as e:
        print(f"❌ Error summarizing with Gemini: {e}")
        return None


def _parse_schema_from_sql(sql: str) -> str:
    """
    Parse schema name (gold/platinum) from SQL query
    
    Args:
        sql: SQL query string
        
    Returns:
        Schema name (gold or platinum), default to 'gold'
    """
    if not sql:
        return "gold"
    
    sql_lower = sql.lower()
    
    # Check for platinum first (more specific)
    if "platinum" in sql_lower:
        return "platinum"
    elif "gold" in sql_lower:
        return "gold"
    else:
        # Default to gold
        return "gold"


def format_answer(
    question: str,
    sql_query: Optional[str],
    rows_preview: Optional[List[Dict]],
    citations: Optional[List[Dict]],
    execution_time_ms: int,
    error: Optional[str] = None,
    source_schema: Optional[str] = None,
    suggestions: Optional[List[str]] = None
) -> str:
    """
    Format complete answer with header, summary, and suggestions
    
    Args:
        question: Original question
        sql_query: Executed SQL query
        rows_preview: Query results
        citations: RAG citations
        execution_time_ms: Execution time
        error: Error message if any
        source_schema: Source schema (gold/platinum), auto-parsed if None
        suggestions: List of suggestion strings
        
    Returns:
        Formatted answer text
    """
    answer_parts = []
    
    if error:
        # Error case: don't add header, just show error with suggestions
        answer_parts.append(error)
        return "\n".join(answer_parts)
    
    # Success case: Add header with data provenance
    if sql_query and rows_preview:
        # Parse schema if not provided
        if not source_schema:
            source_schema = _parse_schema_from_sql(sql_query)
        
        # Data freshness message (fixed for batch Olist data)
        data_freshness = "Dữ liệu batch (2016-2018), không realtime"
        
        # Header with data provenance
        header = f"🗂️ **Nguồn:** `lakehouse.{source_schema}` • ⏱️ **Thời gian chạy:** {execution_time_ms}ms • 📦 {data_freshness}"
        answer_parts.append(header)
        answer_parts.append("")  # Empty line
        
        # Try to get Gemini summary
        summary = summarize_with_gemini(question, rows_preview, citations)
        
        if summary:
            answer_parts.append("📝 **Tóm tắt:**")
            answer_parts.append(summary)
            answer_parts.append("")  # Empty line
        else:
            # Fallback: brief info about results
            answer_parts.append(f"📊 **Kết quả:** {len(rows_preview)} dòng")
            answer_parts.append("")  # Empty line
    
    # Add citations (if any)
    if citations and len(citations) > 0:
        answer_parts.append("📚 **Tài liệu tham khảo:**")
        for cite in citations[:3]:  # Show top 3 citations
            answer_parts.append(
                f"  • {cite.get('source', 'unknown')} "
                f"(độ liên quan: {cite.get('score', 0):.2f})"
            )
        answer_parts.append("")  # Empty line
    
    # Note: suggestions are handled separately in AskResponse model
    # They will be displayed as buttons in the UI
    
    return "\n".join(answer_parts)


if __name__ == "__main__":
    # Test summarization
    test_question = "Doanh thu theo tháng 3 tháng gần đây?"
    
    test_data = [
        {"month": "2018-08-01", "revenue": 1234567.89, "orders": 5432},
        {"month": "2018-07-01", "revenue": 1123456.78, "orders": 4987},
        {"month": "2018-06-01", "revenue": 987654.32, "orders": 4123},
    ]
    
    test_citations = [
        {"source": "data_dictionary.md", "score": 0.89, "text": "revenue = total_price"},
        {"source": "kpi_definitions.md", "score": 0.76, "text": "Revenue KPIs"},
    ]
    
    print("="*60)
    print("Testing Gemini Summarization")
    print("="*60)
    
    summary = summarize_with_gemini(test_question, test_data, test_citations)
    
    if summary:
        print(f"\n✅ Summary:\n{summary}")
    else:
        print("\n❌ No summary generated (LLM_PROVIDER not set to gemini?)")
    
    print("\n" + "="*60)
    print("Testing Full Answer Formatting")
    print("="*60)
    
    answer = format_answer(
        question=test_question,
        sql_query="SELECT ... FROM ...",
        rows_preview=test_data,
        citations=test_citations,
        execution_time_ms=234
    )
    
    print(f"\n{answer}")

