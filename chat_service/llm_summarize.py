# -*- coding: utf-8 -*-
"""
LLM-based result summarization module using Google Gemini
"""
import os
from typing import List, Dict, Optional
import google.generativeai as genai


PROMPT_SUMMARY = """Bạn là nhà phân tích dữ liệu chuyên nghiệp. Tóm tắt kết quả truy vấn NGẮN GỌN (2-5 câu) bằng tiếng Việt, có nêu số liệu nổi bật.

ĐẦU VÀO:
- Câu hỏi: {question}
- Bảng dữ liệu (tối đa 50 hàng): 
{table_preview}

{citations_section}

YÊU CẦU:
- Nếu số liệu có top/bottom rõ ràng, hãy nêu cụ thể (ví dụ: "Top 3 là X, Y, Z với giá trị A, B, C").
- Không bịa số. Chỉ dùng số liệu có trong bảng.
- Không liệt kê quá dài. Tối đa 5 dòng.
- Kết luận rõ ràng trong 1 câu cuối.
- Nếu có xu hướng (tăng/giảm), hãy nêu.

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


def format_answer(
    question: str,
    sql_query: Optional[str],
    rows_preview: Optional[List[Dict]],
    citations: Optional[List[Dict]],
    execution_time_ms: int,
    error: Optional[str] = None
) -> str:
    """
    Format complete answer with optional Gemini summarization
    
    Args:
        question: Original question
        sql_query: Executed SQL query
        rows_preview: Query results
        citations: RAG citations
        execution_time_ms: Execution time
        error: Error message if any
        
    Returns:
        Formatted answer text
    """
    answer_parts = []
    
    if error:
        answer_parts.append(f"❌ Lỗi: {error}")
        
        # Suggest examples
        answer_parts.append("\n💡 Hãy thử các câu hỏi sau:")
        answer_parts.append("  • Doanh thu theo tháng 3 tháng gần đây?")
        answer_parts.append("  • Top 10 sản phẩm bán chạy nhất?")
        answer_parts.append("  • Phương thức thanh toán nào phổ biến nhất?")
        
        return "\n".join(answer_parts)
    
    # Success case
    if rows_preview:
        answer_parts.append(f"✅ Đã thực thi SQL query thành công")
        answer_parts.append(f"⏱️  Thời gian: {execution_time_ms}ms")
        answer_parts.append(f"📊 Kết quả: {len(rows_preview)} dòng\n")
        
        # Try to get Gemini summary
        summary = summarize_with_gemini(question, rows_preview, citations)
        
        if summary:
            answer_parts.append("📝 **Tóm tắt:**")
            answer_parts.append(summary)
        else:
            # Fallback: show first few rows
            if rows_preview and len(rows_preview) > 0:
                answer_parts.append(f"💡 Ví dụ dòng đầu tiên:")
                answer_parts.append(f"```json\n{rows_preview[0]}\n```")
    
    # Add citations
    if citations and len(citations) > 0:
        answer_parts.append(f"\n📚 Tài liệu tham khảo:")
        for cite in citations[:4]:
            answer_parts.append(
                f"  - {cite.get('source', 'unknown')} "
                f"(độ liên quan: {cite.get('score', 0):.2f})"
            )
    
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

