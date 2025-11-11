"""
Intent-based SQL templates for common queries
Safe, predefined SQL patterns for Brazilian E-commerce data
"""
import re
from typing import Optional, Tuple, Dict


# Help/general query triggers
HELP_TRIGGERS = [
    "thông tin gì", "có thể cung cấp", "có thể làm gì", "làm được gì",
    "what can you do", "help", "hỗ trợ", "gợi ý", "menu", "khả năng",
    "giúp gì", "biết gì", "có gì", "có những gì", "bạn là ai",
    "giới thiệu", "chức năng"
]

# Small talk triggers (greetings, thanks, goodbye, personal questions)
SMALLTALK_TRIGGERS = [
    "xin chào", "chào", "hello", "hi", "hey",
    "cảm ơn", "thanks", "thank you", "thank",
    "tạm biệt", "bye", "goodbye", "see you",
    "chào bạn", "chào mừng", "welcome",
    "bạn là ai", "bạn biết tôi", "tôi là ai",
    "who are you", "who am i", "what is your name",
    "tên bạn", "tên mình", "bạn tên gì"
]

# About data triggers (dataset questions)
ABOUT_DATA_TRIGGERS = [
    "dữ liệu", "dataset", "data", "batch",
    "lakehouse data", "bộ dữ liệu", "dữ liệu gì",
    "dữ liệu của", "dataset của", "data của",
    "dữ liệu đã xử lý", "dữ liệu gồm", "dữ liệu nào",
    "thông tin dữ liệu", "dữ liệu có gì"
]

# About project triggers (architecture/technology questions)
ABOUT_PROJECT_TRIGGERS = [
    "kiến trúc", "công nghệ", "project", "đồ án",
    "stack", "dịch vụ", "technology", "architecture",
    "công nghệ gì", "dùng công nghệ", "tech stack",
    "kiến trúc hệ thống", "hệ thống dùng", "công nghệ nào",
    "đồ án dùng", "project dùng", "stack nào"
]

# Sentinel value for non-SQL responses
NO_SQL = "__NO_SQL__"


def _has_data_entities(question: str) -> bool:
    """
    Check if question contains data-related entities (time, dimension, measure)
    
    Args:
        question: User's question
        
    Returns:
        True if question contains data entities
    """
    q = question.lower()
    
    # Time-related keywords
    time_keywords = ["tháng", "month", "ngày", "day", "năm", "year", "tuần", "week", 
                     "quý", "quarter", "gần đây", "recent", "2016", "2017", "2018"]
    
    # Dimension keywords
    dimension_keywords = ["sản phẩm", "product", "danh mục", "category", "khách hàng", 
                          "customer", "người bán", "seller", "vùng", "region", "bang", "state",
                          "thanh toán", "payment", "đơn hàng", "order"]
    
    # Measure keywords
    measure_keywords = ["doanh thu", "revenue", "gmv", "orders", "đơn", "units", "aov",
                        "tổng", "sum", "trung bình", "average", "số lượng", "count"]
    
    # Check if question contains any data-related keywords
    has_time = any(kw in q for kw in time_keywords)
    has_dimension = any(kw in q for kw in dimension_keywords)
    has_measure = any(kw in q for kw in measure_keywords)
    
    return has_time or has_dimension or has_measure


def intent_to_sql(question: str) -> Tuple[Optional[str], Optional[Dict]]:
    """
    Convert natural language question to SQL query using template matching
    or return non-SQL response metadata
    
    Args:
        question: User's question in Vietnamese or English
        
    Returns:
        Tuple of (SQL string or NO_SQL sentinel or empty string, metadata dict or None)
        - SQL string: Normal SQL query
        - Empty string "": HELP mode
        - NO_SQL: Non-SQL response (smalltalk, about_data, about_project)
        - None: No match found
        - metadata: Dict with topic info for non-SQL responses
    """
    q = question.lower().strip()
    
    # 1. Check SMALLTALK triggers first (highest priority)
    if any(trigger in q for trigger in SMALLTALK_TRIGGERS):
        return NO_SQL, {"topic": "smalltalk"}
    
    # 2. Check ABOUT_DATA triggers
    if any(trigger in q for trigger in ABOUT_DATA_TRIGGERS):
        return NO_SQL, {"topic": "about_data"}
    
    # 3. Check ABOUT_PROJECT triggers
    if any(trigger in q for trigger in ABOUT_PROJECT_TRIGGERS):
        return NO_SQL, {"topic": "about_project"}
    
    # 4. Fallback: Check for non-SQL intent (personal questions without data entities)
    # If question doesn't contain data entities and has social keywords, treat as smalltalk
    social_keywords = ["bạn", "tôi", "mình", "ai", "tên", "tuổi", "you", "i", "who", "what is your", "what is my"]
    has_social = any(kw in q for kw in social_keywords)
    has_data = _has_data_entities(question)
    
    if has_social and not has_data:
        # Personal question without data context → smalltalk
        return NO_SQL, {"topic": "smalltalk"}
    
    # 5. Detect HELP intent (general/open questions)
    # Return empty string "" as signal for HELP MODE
    if any(trigger in q for trigger in HELP_TRIGGERS):
        return "", None
    
    # Revenue by month
    # NOTE: Prefer using router/skill (RevenueTimeseriesSkill) instead of this template
    # This template is only used as fallback for simple queries
    if any(kw in q for kw in ["doanh thu", "revenue"]) and any(kw in q for kw in ["tháng", "month"]):
        # Try to use platinum datamart first (faster, pre-aggregated)
        # Only fallback to gold.fact_order if needed
        
        # Check if question has specific time range - if yes, let router/skill handle it
        has_time_range = any(kw in q for kw in ["từ", "from", "đến", "to", "between", "giữa", "năm", "year", "2016", "2017", "2018"])
        
        if has_time_range:
            # Has time range -> let router/skill handle it (return None to fall through)
            return None, None
        
        # For "gần đây" or no time indicator, use platinum datamart (fast, pre-aggregated)
        # Default: last 12 months from datamart (covers all available data 2016-2018)
        # For Olist data, use 2017-2018 range (latest available)
        return """
        SELECT 
            year_month AS month,
            SUM(gmv) AS revenue,
            SUM(orders) AS order_count,
            SUM(units) AS units
        FROM lakehouse.platinum.dm_sales_monthly_category
        WHERE year_month >= '2017-01'
          AND year_month <= '2018-12'
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 200
        """, None
    
    # Top products by revenue
    if any(kw in q for kw in ["top", "cao nhất"]) and any(kw in q for kw in ["sản phẩm", "product"]):
        limit = 10
        # Extract number if present
        nums = re.findall(r'\d+', q)
        if nums:
            limit = min(int(nums[0]), 50)  # Max 50
        
        # Check if user wants by units or revenue
        metric = 'revenue' if 'doanh thu' in q or 'revenue' in q else 'units'
        order_by_col = 'gmv' if metric == 'revenue' else 'units'
        
        return f"""
        WITH item AS (
          SELECT
            oi.product_id,
            SUM(oi.price + oi.freight_value) AS gmv,
            COUNT(*) AS units,
            COUNT(DISTINCT oi.order_id) AS orders
          FROM lakehouse.gold.fact_order_item oi
          WHERE oi.full_date IS NOT NULL
          GROUP BY oi.product_id
        )
        SELECT
          i.product_id,
          COALESCE(pc.product_category_name_english, 'unknown') AS category_en,
          p.product_weight_g,
          p.product_length_cm,
          p.product_height_cm,
          p.product_width_cm,
          i.orders,
          i.units,
          ROUND(i.gmv, 2) AS gmv,
          ROUND(i.gmv / NULLIF(i.orders, 0), 2) AS aov
        FROM item i
        JOIN lakehouse.gold.dim_product p
          ON p.product_id = i.product_id
        LEFT JOIN lakehouse.gold.dim_product_category pc
          ON pc.product_category_name = p.product_category_name
        ORDER BY {order_by_col} DESC
        LIMIT {limit}
        """, None
    
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
        """, None
    
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
        """, None
    
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
        """, None
    
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
        """, None
    
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
        """, None
    
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
        """, None
    
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
        """, None
    
    return None, None


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

