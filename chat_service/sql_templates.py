"""
Intent-based SQL templates for common queries
Safe, predefined SQL patterns for Brazilian E-commerce data
"""
import re
from typing import Optional


# Help/general query triggers
HELP_TRIGGERS = [
    "thông tin gì", "có thể cung cấp", "có thể làm gì", "làm được gì",
    "what can you do", "help", "hỗ trợ", "gợi ý", "menu", "khả năng",
    "giúp gì", "biết gì", "có gì", "có những gì", "bạn là ai",
    "giới thiệu", "chức năng"
]


def intent_to_sql(question: str) -> Optional[str]:
    """
    Convert natural language question to SQL query using template matching
    
    Args:
        question: User's question in Vietnamese or English
        
    Returns:
        SQL string, empty string (for HELP mode), or None if no match
    """
    q = question.lower().strip()
    
    # Detect HELP intent (general/open questions)
    # Return empty string "" as signal for HELP MODE
    if any(trigger in q for trigger in HELP_TRIGGERS):
        return ""
    
    # Revenue by month
    if any(kw in q for kw in ["doanh thu", "revenue"]) and any(kw in q for kw in ["tháng", "month"]):
        return """
        SELECT 
            date_trunc('month', full_date) AS month,
            SUM(payment_total) AS revenue,
            COUNT(DISTINCT order_id) AS order_count,
            COUNT(DISTINCT customer_id) AS customer_count
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 200
        """
    
    # Top products by revenue
    if any(kw in q for kw in ["top", "cao nhất"]) and any(kw in q for kw in ["sản phẩm", "product"]):
        limit = 10
        # Extract number if present
        nums = re.findall(r'\d+', q)
        if nums:
            limit = min(int(nums[0]), 50)  # Max 50
        
        return f"""
        SELECT 
            order_id,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(payment_total) AS total_revenue
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
        GROUP BY order_id
        ORDER BY total_revenue DESC
        LIMIT {limit}
        """
    
    # Orders by region/state
    if any(kw in q for kw in ["vùng", "region", "khu vực", "tỉnh", "state"]):
        return """
        SELECT 
            customer_id,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(payment_total) AS total_revenue,
            AVG(payment_total) AS avg_order_value
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
        GROUP BY customer_id
        ORDER BY total_revenue DESC
        LIMIT 30
        """
    
    # Orders by payment type
    if any(kw in q for kw in ["thanh toán", "payment"]):
        return """
        SELECT 
            primary_payment_type AS payment_type,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(payment_total) AS total_revenue,
            AVG(payment_total) AS avg_order_value
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
          AND primary_payment_type IS NOT NULL
        GROUP BY primary_payment_type
        ORDER BY order_count DESC
        LIMIT 200
        """
    
    # Top categories
    if any(kw in q for kw in ["danh mục", "category", "loại"]):
        return """
        SELECT 
            order_id,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(payment_total) AS total_revenue,
            SUM(items_count) AS total_quantity,
            AVG(payment_total) AS avg_revenue_per_order
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
        GROUP BY order_id
        ORDER BY total_revenue DESC
        LIMIT 20
        """
    
    # Order status breakdown
    if any(kw in q for kw in ["trạng thái", "status", "đơn hàng"]) and any(kw in q for kw in ["phân bố", "breakdown"]):
        return """
        SELECT 
            CASE 
                WHEN is_canceled = TRUE THEN 'canceled'
                WHEN delivered_on_time = TRUE THEN 'delivered_on_time'
                WHEN delivered_on_time = FALSE THEN 'delivered_late'
                ELSE 'other'
            END AS order_status,
            COUNT(DISTINCT order_id) AS order_count,
            COUNT(DISTINCT customer_id) AS customer_count,
            ROUND(COUNT(DISTINCT order_id) * 100.0 / SUM(COUNT(DISTINCT order_id)) OVER(), 2) AS percentage
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
        GROUP BY 1
        ORDER BY order_count DESC
        LIMIT 200
        """
    
    # Recent orders
    if any(kw in q for kw in ["gần đây", "recent", "mới nhất", "latest", "đơn hàng"]):
        return """
        SELECT 
            order_id,
            customer_id,
            full_date,
            payment_total,
            primary_payment_type,
            items_count,
            delivered_on_time,
            is_canceled
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
        ORDER BY full_date DESC
        LIMIT 50
        """
    
    # Average order value
    if any(kw in q for kw in ["trung bình", "average", "aov"]) and any(kw in q for kw in ["đơn", "order"]):
        return """
        SELECT 
            date_trunc('month', full_date) AS month,
            AVG(payment_total) AS avg_order_value,
            STDDEV(payment_total) AS stddev_order_value,
            MIN(payment_total) AS min_order_value,
            MAX(payment_total) AS max_order_value,
            COUNT(DISTINCT order_id) AS order_count
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 200
        """
    
    # Delivery performance
    if any(kw in q for kw in ["giao hàng", "delivery", "vận chuyển"]):
        return """
        SELECT 
            CASE 
                WHEN delivered_on_time = TRUE THEN 'On Time'
                WHEN delivered_on_time = FALSE THEN 'Late'
                ELSE 'Unknown'
            END AS delivery_status,
            COUNT(DISTINCT order_id) AS order_count,
            AVG(delivered_days) AS avg_delivery_days
        FROM lakehouse.gold.fact_order
        WHERE full_date IS NOT NULL
          AND delivered_days IS NOT NULL
        GROUP BY 1
        ORDER BY order_count DESC
        LIMIT 200
        """
    
    return None


def get_safe_schemas() -> list:
    """Return list of safe schemas for querying"""
    return ["gold", "platinum"]


def get_example_questions() -> list:
    """Return list of example questions for UI"""
    return [
        "Doanh thu theo tháng gần đây?",
        "Top 10 sản phẩm bán chạy nhất?",
        "Phân bố đơn hàng theo vùng miền?",
        "Phương thức thanh toán nào phổ biến nhất?",
        "Danh mục sản phẩm nào có doanh thu cao nhất?",
        "Giá trị trung bình của đơn hàng theo tháng?",
        "Tỷ lệ giao hàng đúng hạn?",
        "Trạng thái đơn hàng hiện tại?",
        "50 đơn hàng gần nhất?",
    ]

