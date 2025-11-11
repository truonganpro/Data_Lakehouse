"""
Context-aware suggestions based on skill/intent and query results
"""
from typing import List, Optional, Dict

# Import GuardCode from errors module
try:
    from errors import GuardCode
except ImportError:
    # Fallback for when running outside package
    from chat_service.errors import GuardCode


def suggestions_for(
    skill_meta: Optional[Dict] = None,
    rows_preview: Optional[List[Dict]] = None,
    guard_code: Optional[GuardCode] = None,
    question: Optional[str] = None
) -> List[str]:
    """
    Generate context-aware suggestions based on skill metadata, results, and error code
    
    Args:
        skill_meta: Metadata from skill/router (skill_name, params, etc.)
        rows_preview: Query results preview
        guard_code: Error code if any (from guardrails)
        question: Original question for context
        
    Returns:
        List of suggestion strings (2-3 suggestions)
    """
    suggestions = []
    
    # Priority 1: If guard_code exists, suggest fixes
    if guard_code:
        if guard_code == GuardCode.MISSING_TIME_PRED:
            return [
                "Doanh thu 3 tháng gần đây",
                "Doanh thu Q3-2018",
                "Doanh thu theo tháng từ 2017-01 đến 2017-12"
            ]
        elif guard_code == GuardCode.MISSING_LIMIT:
            return [
                "Top 100 đơn hàng gần đây",
                "Doanh thu theo tháng (tổng hợp)",
                "Top 10 sản phẩm bán chạy"
            ]
        elif guard_code == GuardCode.DISALLOWED_SCHEMA:
            return [
                "Doanh thu theo tháng từ datamart",
                "Top 10 sản phẩm bán chạy",
                "Phân tích cohort khách hàng"
            ]
        elif guard_code == GuardCode.NO_DATA:
            return [
                "Mở rộng khoảng thời gian (6 tháng)",
                "Bỏ bớt bộ lọc danh mục",
                "Xem dữ liệu tổng hợp năm 2017-2018"
            ]
        elif guard_code == GuardCode.AMBIGUOUS_INTENT:
            return [
                "Doanh thu theo tháng gần đây",
                "Top 10 sản phẩm bán chạy",
                "Phương thức thanh toán phổ biến"
            ]
    
    # Priority 2: Skill-based suggestions
    if skill_meta:
        skill_name = skill_meta.get("skill_name", "").lower()
        params = skill_meta.get("params", {})
        
        # Revenue timeseries
        if "revenue" in skill_name and "timeseries" in skill_name:
            time_window = params.get("time_window", {})
            return [
                "So sánh doanh thu theo quý",
                "Xem 12 tháng gần đây",
                "Xu hướng doanh thu theo năm"
            ]
        
        # Top products
        if "product" in skill_name or "bestseller" in skill_name:
            category = params.get("entities", {}).get("category")
            if category:
                return [
                    f"Top sản phẩm trong danh mục {category}",
                    "Top sản phẩm theo khu vực",
                    "So sánh với các danh mục khác"
                ]
            return [
                "Top sản phẩm theo danh mục",
                "Top sản phẩm theo khu vực",
                "Top sản phẩm theo tháng"
            ]
        
        # Payment mix
        if "payment" in skill_name:
            return [
                "AOV theo phương thức thanh toán",
                "Xu hướng thanh toán theo tháng",
                "Tỷ lệ thanh toán trả góp"
            ]
        
        # Category revenue
        if "category" in skill_name:
            return [
                "So sánh danh mục theo quý",
                "Top danh mục theo GMV",
                "Xu hướng danh mục theo tháng"
            ]
        
        # Regional distribution
        if "region" in skill_name or "distribution" in skill_name:
            return [
                "GMV theo bang (state)",
                "GMV theo thành phố",
                "Top khu vực theo doanh thu"
            ]
        
        # Seller performance
        if "seller" in skill_name:
            return [
                "Top seller theo GMV",
                "Seller có on-time rate thấp",
                "Seller có review score cao"
            ]
        
        # Cohort retention
        if "cohort" in skill_name or "retention" in skill_name:
            return [
                "Retention sau 3 tháng",
                "Retention sau 6 tháng",
                "So sánh cohort theo tháng"
            ]
        
        # MoM/YoY
        if "mom" in skill_name or "yoy" in skill_name:
            return [
                "Xu hướng tăng trưởng theo quý",
                "So sánh với năm trước",
                "Phân tích theo danh mục"
            ]
        
        # AOV analysis
        if "aov" in skill_name:
            return [
                "AOV theo phương thức thanh toán",
                "AOV theo danh mục",
                "AOV theo tháng"
            ]
        
        # On-time rate
        if "ontime" in skill_name or "sla" in skill_name:
            return [
                "On-time rate theo tháng",
                "On-time rate theo bang",
                "Top seller có on-time rate thấp"
            ]
    
    # Priority 3: Data-driven suggestions (based on rows_preview)
    if rows_preview and len(rows_preview) > 0:
        # Check if data has time dimension
        first_row = rows_preview[0]
        has_month = any("month" in str(k).lower() for k in first_row.keys())
        has_category = any("category" in str(k).lower() for k in first_row.keys())
        has_region = any("region" in str(k).lower() or "state" in str(k).lower() for k in first_row.keys())
        
        if has_month:
            suggestions.append("Xem chi tiết theo quý")
            suggestions.append("So sánh với năm trước")
        if has_category:
            suggestions.append("Xem chi tiết theo danh mục")
        if has_region:
            suggestions.append("Xem chi tiết theo khu vực")
    
    # Priority 4: Question-based fallback
    if question:
        q_lower = question.lower()
        if "tháng" in q_lower or "month" in q_lower:
            suggestions.append("Xem theo quý")
            suggestions.append("Xem theo năm")
        if "sản phẩm" in q_lower or "product" in q_lower:
            suggestions.append("Xem theo danh mục")
            suggestions.append("Xem theo khu vực")
        if "thanh toán" in q_lower or "payment" in q_lower:
            suggestions.append("Xem AOV theo phương thức")
            suggestions.append("Xu hướng theo tháng")
    
    # Default fallback
    if not suggestions:
        suggestions = [
            "Doanh thu 3 tháng gần đây",
            "Top 10 sản phẩm bán chạy",
            "Phương thức thanh toán phổ biến"
        ]
    
    # Return top 3 suggestions
    return suggestions[:3]


def suggestions_for_non_sql(topic: str) -> List[str]:
    """
    Generate suggestions for non-SQL responses (smalltalk, about_data, about_project)
    
    Args:
        topic: Topic type (smalltalk, about_data, about_project)
        
    Returns:
        List of suggestion strings
    """
    if topic == "smalltalk":
        return [
            "Doanh thu 3 tháng gần đây",
            "Top 10 sản phẩm bán chạy",
            "Phương thức thanh toán phổ biến"
        ]
    elif topic == "about_data":
        return [
            "Doanh thu theo tháng",
            "Top sản phẩm bán chạy",
            "Phân tích cohort khách hàng"
        ]
    elif topic == "about_project":
        return [
            "Doanh thu theo tháng",
            "Top sản phẩm bán chạy",
            "Kiến trúc hệ thống"
        ]
    else:
        return [
            "Doanh thu 3 tháng gần đây",
            "Top 10 sản phẩm bán chạy",
            "Phương thức thanh toán phổ biến"
        ]

