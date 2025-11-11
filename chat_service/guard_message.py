"""
Guard error messages and suggestions
Maps error codes to user-friendly messages and quick-reply suggestions
"""
from chat_service.errors import GuardCode
from typing import Tuple, List, Optional, Dict


def message_and_suggestions(
    code: GuardCode, 
    skill_meta: Optional[Dict] = None,
    question: Optional[str] = None
) -> Tuple[str, List[str]]:
    """
    Get user-friendly error message and quick-reply suggestions based on error code
    
    Args:
        code: Guard error code
        skill_meta: Optional metadata from skill/router (e.g., expected_time_col, default_windows)
        question: Optional original question for context
        
    Returns:
        Tuple of (message, suggestions_list)
    """
    
    if code == GuardCode.MISSING_TIME_PRED:
        # Thiếu điều kiện thời gian cho bảng fact lớn
        suggestions = [
            "Doanh thu 3 tháng gần đây",
            "Doanh thu Q3-2018",
            "Doanh thu theo tháng từ 2017-01 đến 2017-12"
        ]
        if skill_meta and "default_windows" in skill_meta:
            # Use skill-specific default windows if available
            windows = skill_meta["default_windows"]
            if windows:
                suggestions = windows[:3]
        
        return (
            "⚠️ **Mình không chạy truy vấn này vì thiếu điều kiện thời gian cho bảng fact lớn.**\n\n"
            "💡 **Gợi ý:** Hãy chỉ định khoảng thời gian cụ thể (ví dụ: 3 tháng gần đây, Q3-2018).",
            suggestions
        )
    
    if code == GuardCode.MISSING_LIMIT:
        # Thiếu LIMIT
        return (
            "⚠️ **Truy vấn thiếu LIMIT nên có thể trả về quá nhiều dòng.**\n\n"
            "💡 **Gợi ý:** Hãy thêm LIMIT hoặc lọc theo thời gian/danh mục.",
            [
                "Top 100 đơn hàng gần đây",
                "Doanh thu theo tháng (tổng hợp)",
                "Top 10 sản phẩm bán chạy"
            ]
        )
    
    if code == GuardCode.DISALLOWED_SCHEMA:
        # Schema ngoài whitelist
        return (
            "⚠️ **Câu hỏi đang chạm vào schema ngoài vùng an toàn (chỉ gold/platinum).**\n\n"
            "💡 **Gợi ý:** Hãy truy vấn trong `lakehouse.gold` hoặc `lakehouse.platinum`.",
            [
                "Doanh thu theo tháng từ datamart",
                "Top 10 sản phẩm bán chạy",
                "Phân tích cohort khách hàng"
            ]
        )
    
    if code == GuardCode.STAR_PROJECTION:
        # SELECT * không được phép
        return (
            "⚠️ **Không cho phép `SELECT *`. Hãy chọn cột cụ thể để an toàn và nhanh hơn.**\n\n"
            "💡 **Gợi ý:** Hãy chỉ định các cột cần thiết (ví dụ: month, revenue, order_count).",
            [
                "Doanh thu theo tháng: month, revenue, order_count",
                "Top sản phẩm: product_id, category, gmv",
                "Phân bố thanh toán: payment_type, orders, total"
            ]
        )
    
    if code == GuardCode.NO_DATA:
        # Không có dữ liệu
        suggestions = [
            "Mở rộng khoảng thời gian (6 tháng)",
            "Bỏ bớt bộ lọc danh mục",
            "Xem dữ liệu tổng hợp"
        ]
        if skill_meta and "expected_time_col" in skill_meta:
            time_col = skill_meta["expected_time_col"]
            if "year_month" in time_col:
                suggestions.insert(0, f"Xem dữ liệu năm 2017-2018")
            else:
                suggestions.insert(0, f"Xem dữ liệu từ 2017-01-01 đến 2018-10-17")
        
        return (
            "📭 **Không có dữ liệu khớp điều kiện hiện tại.**\n\n"
            "💡 **Gợi ý:** Hãy thử mở rộng khoảng thời gian hoặc bỏ bớt bộ lọc.",
            suggestions
        )
    
    if code == GuardCode.BANNED_FUNC:
        # Hàm/stmt cấm
        return (
            "⚠️ **Câu lệnh chứa hàm hoặc statement không được phép.**\n\n"
            "💡 **Gợi ý:** Chỉ SELECT và WITH queries được phép (read-only).",
            [
                "Doanh thu theo tháng",
                "Top 10 sản phẩm bán chạy",
                "Phân tích thanh toán"
            ]
        )
    
    if code == GuardCode.AMBIGUOUS_INTENT:
        # Câu hỏi mơ hồ
        return (
            "❓ **Mình cần rõ hơn bạn muốn xem chỉ số nào.**\n\n"
            "💡 **Gợi ý:** Hãy chỉ định cụ thể metric, thời gian, và dimension.",
            [
                "Doanh thu theo tháng gần đây",
                "Phương thức thanh toán phổ biến",
                "Top 10 sản phẩm bán chạy",
                "Tỷ lệ giao hàng đúng hạn"
            ]
        )
    
    if code == GuardCode.NON_SQL_INTENT:
        # Non-SQL intent (small talk, about data, about project)
        # This should be handled separately, but include as fallback
        return (
            "💬 **Đây là câu hỏi không cần SQL.**\n\n"
            "💡 **Mình có thể:**\n"
            "  • Chào hỏi và giới thiệu\n"
            "  • Giới thiệu dataset đã xử lý\n"
            "  • Tóm tắt kiến trúc đồ án",
            [
                "Giới thiệu dataset đã xử lý",
                "Tóm tắt kiến trúc đồ án",
                "Doanh thu 3 tháng gần đây"
            ]
        )
    
    # Default fallback
    return (
        "⚠️ **Mình chưa sinh được SQL an toàn cho câu hỏi này.**\n\n"
        "💡 **Gợi ý:** Hãy thử một trong các câu hỏi sau:",
        [
            "Doanh thu 3 tháng gần đây",
            "Top 10 sản phẩm bán chạy",
            "Phân bố đơn hàng theo vùng",
            "Phương thức thanh toán phổ biến"
        ]
    )

