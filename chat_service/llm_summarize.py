# -*- coding: utf-8 -*-
"""
LLM-based result summarization module using Google Gemini
"""
import os
import re
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


def _explain_sql_and_lineage(sql: str, source_schema: str, rows_preview: Optional[List[Dict]]) -> str:
    """
    Explain SQL calculation and data lineage
    
    Args:
        sql: SQL query string
        source_schema: Source schema (gold/platinum)
        rows_preview: Query results (to infer measures)
        
    Returns:
        Explanation text or empty string
    """
    if not sql:
        return ""
    
    sql_lower = sql.lower()
    explain_parts = []
    
    # Detect main table
    main_table = None
    if "from" in sql_lower:
        # Simple extraction: FROM table_name or FROM schema.table_name
        from_match = re.search(r'from\s+[\w.]+\.([\w]+)', sql_lower)
        if from_match:
            main_table = from_match.group(1)
        else:
            from_match = re.search(r'from\s+([\w]+)', sql_lower)
            if from_match:
                main_table = from_match.group(1)
    
    # Detect measures (SUM, COUNT, AVG, etc.)
    measures = []
    if "sum(" in sql_lower:
        measures.append("tổng")
    if "count(" in sql_lower or "count(*)" in sql_lower:
        measures.append("số lượng")
    if "avg(" in sql_lower or "average(" in sql_lower:
        measures.append("trung bình")
    
    # Detect dimensions (GROUP BY)
    dimensions = []
    if "group by" in sql_lower:
        group_by_match = re.search(r'group\s+by\s+([^order\s]+)', sql_lower, re.IGNORECASE)
        if group_by_match:
            group_cols = group_by_match.group(1).strip()
            # Extract column names (simple heuristic)
            for col in group_cols.split(','):
                col = col.strip().split()[-1]  # Get last word (column name)
                if col and col not in ['1', '2', '3', '4', '5']:  # Skip positional numbers
                    dimensions.append(col)
    
    # Build explanation
    if main_table:
        # Determine layer
        if source_schema == "platinum":
            layer_desc = "datamart tổng hợp (pre-aggregated)"
            lineage = f"Bronze → Silver → Gold → Platinum (`{main_table}`)"
        else:
            layer_desc = "fact/dimension tables"
            if "fact_" in main_table:
                lineage = f"Bronze → Silver → Gold (`{main_table}`)"
            elif "dim_" in main_table:
                lineage = f"Bronze → Silver → Gold (`{main_table}` - dimension table)"
            else:
                lineage = f"Bronze → Silver → Gold (`{main_table}`)"
        
        explain_parts.append(f"• **Nguồn dữ liệu**: `lakehouse.{source_schema}.{main_table}` ({layer_desc})")
        explain_parts.append(f"• **Lineage**: {lineage}")
    
    # Explain measures if detected
    if measures:
        measure_desc = ", ".join(measures)
        explain_parts.append(f"• **Phép tính**: {measure_desc}")
    
    # Explain dimensions if detected
    if dimensions and len(dimensions) <= 3:
        dim_desc = ", ".join(dimensions[:3])
        explain_parts.append(f"• **Nhóm theo**: {dim_desc}")
    
    # Add KPI explanations for common measures
    if rows_preview and len(rows_preview) > 0:
        columns = list(rows_preview[0].keys())
        
        # Check for common KPIs
        kpi_explanations = {
            "gmv": "GMV = tổng (price × quantity + freight_value)",
            "revenue": "Revenue = tổng (price × quantity + freight_value)",
            "aov": "AOV = GMV / số đơn hàng",
            "orders": "Orders = số lượng đơn hàng duy nhất",
            "units": "Units = tổng số lượng sản phẩm",
            "retention": "Retention = (khách hàng active / cohort size) × 100%",
            "on_time_rate": "On-time rate = (đơn giao đúng hạn / tổng đơn) × 100%"
        }
        
        for col in columns:
            col_lower = col.lower()
            for kpi, explanation in kpi_explanations.items():
                if kpi in col_lower:
                    explain_parts.append(f"• **{col}**: {explanation}")
                    break
    
    return "\n".join(explain_parts) if explain_parts else ""


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
        
        # Add SQL explanation and lineage (Việc D)
        explain_text = _explain_sql_and_lineage(sql_query, source_schema, rows_preview)
        if explain_text:
            answer_parts.append("🧠 **Cách tính:**")
            answer_parts.append(explain_text)
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

