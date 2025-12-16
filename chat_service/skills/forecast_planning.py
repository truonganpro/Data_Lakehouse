# -*- coding: utf-8 -*-
"""Forecast planning skill - dự báo doanh thu / số lượng theo horizon"""

from .base import BaseSkill
from typing import Dict, Optional
import re
import unicodedata


class ForecastPlanningSkill(BaseSkill):
    """
    Dự báo doanh thu / nhu cầu trong N ngày tới theo bang / category.

    Ví dụ câu hỏi:
    - "Dự báo doanh thu 7 ngày tới cho từng bang?"
    - "Dự báo nhu cầu 14 ngày tới cho category watches_gifts ở bang SP?"
    """
    
    def __init__(self):
        super().__init__()
        # Đặt priority cao hơn các skill khác một chút
        self.priority = 95
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        has_forecast_kw = any(
            kw in q
            for kw in ["dự báo", "forecast", "horizon", "ci ", "kịch bản", "planning", "yhat", "forecast"]
        )
        
        # Phải có từ liên quan đến thời gian tương lai ("ngày tới", "tuần tới", …)
        has_future_kw = any(
            kw in q
            for kw in ["ngày tới", "ngày tiếp theo", "tuần tới", "7 ngày", "14 ngày", "28 ngày", "tới", "sắp tới"]
        )
        
        # Nếu nói rõ "dữ liệu / bảng dự báo" thì vẫn coi là forecast planning,
        # dù không nhắc "ngày tới"
        has_data_kw = any(kw in q for kw in ["dữ liệu", "bảng", "table", "xem", "chi tiết"])
        
        # Nếu có cả forecast và future keywords → ưu tiên cao nhất
        if has_forecast_kw and has_future_kw:
            # Score cao hơn RevenueTimeseriesSkill (0.95) để được chọn
            return 0.96
        
        # NEW: cho các câu kiểu "xem dữ liệu dự báo" - ưu tiên cao để match trước các skill khác
        if any(kw in q for kw in ["xem dữ liệu", "dữ liệu dự báo", "bảng dự báo", "table forecast", 
                                   "dữ liệu đã dự báo", "dữ liệu bạn đã dự báo", "dữ liệu được dự báo"]):
            if "dự báo" in q or "forecast" in q:
                return 0.89
        
        # Nếu có forecast + "dữ liệu/bảng/xem" → preview mode (score cao để chọn skill này)
        if has_forecast_kw and has_data_kw:
            return 0.88
        
        # Câu kiểu "kịch bản thận trọng và lạc quan khác nhau bao nhiêu %?"
        if "kịch bản" in q or "thận trọng" in q or "lạc quan" in q:
            return 0.85
        
        # Nếu chỉ có forecast keyword (không có future) → vẫn match nhưng thấp hơn
        if has_forecast_kw:
            return 0.7
        
        return 0.0
    
    def _extract_horizon_days(self, question: str, default: int = 7) -> int:
        """Tìm '7 ngày', '14 ngày' trong câu; nếu không thấy thì mặc định 7"""
        q = question.lower()
        m = re.search(r"(\d+)\s*ngày", q)
        if m:
            try:
                days = int(m.group(1))
                return max(1, min(days, 28))  # clamp 1–28
            except ValueError:
                return default
        return default
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text: lowercase, remove Vietnamese diacritics"""
        # Chuyển về lowercase
        text = text.lower()
        # Bỏ dấu tiếng Việt
        text = unicodedata.normalize('NFD', text)
        text = text.encode('ascii', 'ignore').decode('ascii')
        return text
    
    def _extract_category(self, question: str) -> Optional[str]:
        """
        Extract category name từ câu hỏi với mapping từ synonyms sang DB value.
        Returns: category name trong DB (vd: 'computers_accessories') hoặc None
        """
        q_norm = self._normalize_text(question)
        
        # Dictionary mapping: synonyms -> DB value
        category_map = {
            # Computers
            'may tinh': 'computers_accessories',
            'computer': 'computers_accessories',
            'computers': 'computers_accessories',
            'pc': 'computers_accessories',
            'may vi tinh': 'computers_accessories',
            # Health & Beauty
            'suc khoe': 'health_beauty',
            'lam dep': 'health_beauty',
            'health': 'health_beauty',
            'beauty': 'health_beauty',
            'my pham': 'health_beauty',
            # Watches & Gifts
            'dong ho': 'watches_gifts',
            'watches': 'watches_gifts',
            'gifts': 'watches_gifts',
            'qua tang': 'watches_gifts',
            # Books
            'sach': 'books_general_interest',
            'books': 'books_general_interest',
            # Electronics
            'dien thoai': 'telephony',
            'telephony': 'telephony',
            'smartphone': 'telephony',
            # Furniture
            'noi that': 'furniture_decor',
            'furniture': 'furniture_decor',
            'decor': 'furniture_decor',
            # Sports
            'the thao': 'sports_leisure',
            'sports': 'sports_leisure',
            'leisure': 'sports_leisure',
        }
        
        # ✅ FIX: Loại bỏ các từ không phải category (như "từng bang", "từng region")
        exclude_patterns = ['tung bang', 'tung region', 'tung vung', 'cho tung', 'theo tung']
        for exclude in exclude_patterns:
            if exclude in q_norm:
                # Nếu có các từ này, không phải category
                return None
        
        # Patterns để bắt category: "danh mục X", "category X", "cho danh mục X"
        patterns = [
            r'(?:danh\s*muc|nhom|loai|category)\s+([a-z0-9\s_]+)',  # "danh mục computers"
            r'cho\s+(?:danh\s*muc|category|loai)\s+([a-z0-9\s_]+)',  # "cho danh mục computers"
            r'du\s*bao.*cho\s+danh\s*muc\s+([a-z0-9\s_]+)',  # "dự báo cho danh mục computers"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, q_norm)
            if match:
                raw_cat = match.group(1).strip()
                
                # Loại bỏ các từ không phải category
                if any(exclude in raw_cat for exclude in ['bang', 'region', 'vung', 'state', 'tung']):
                    continue
                
                # Kiểm tra xem từ khóa bắt được có trong map không
                for key, val in category_map.items():
                    if key in raw_cat:
                        return val
                
                # Nếu không có trong map, thử dùng trực tiếp (có thể là tên đầy đủ như computers_accessories)
                # Loại bỏ khoảng trắng và kiểm tra format
                raw_cat_clean = raw_cat.replace(' ', '_')
                # Chỉ chấp nhận nếu có dấu gạch dưới (như computers_accessories) hoặc là từ tiếng Anh hợp lệ
                if '_' in raw_cat_clean or (raw_cat_clean.isalpha() and len(raw_cat_clean) > 3):
                    # Kiểm tra lại không phải là từ khóa region
                    if raw_cat_clean not in ['bang', 'region', 'vung', 'state', 'tung']:
                        return raw_cat_clean
        
        return None
    
    def _extract_region(self, question: str) -> Optional[str]:
        """
        Extract region code từ câu hỏi.
        Returns: region code (vd: 'SP', 'RJ') hoặc None
        """
        q = question.lower()
        # Pattern: mã bang 2 chữ cái (SP, RJ, MG, ...)
        m_region = re.search(r"\b(sp|rj|mg|pr|sc|rs|ba|go|pe|ce|df|es|ma|ms|mt|pa|pb|pi|rn|ro|se|to)\b", q)
        if m_region:
            return m_region.group(1).upper()
        return None
    
    def _detect_metric_table(self, question: str) -> str:
        """
        Chọn bảng revenue hay quantity.
        
        - Nếu có 'doanh thu', 'revenue', 'gmv' → bảng _revenue
        - Nếu có 'số lượng', 'nhu cầu', 'quantity' → bảng _quantity
        - Không nói gì → mặc định doanh thu
        """
        q = question.lower()
        if any(kw in q for kw in ["số lượng", "quantity", "nhu cầu"]):
            return "lakehouse.platinum.demand_forecast_quantity"
        # default: revenue
        return "lakehouse.platinum.demand_forecast_revenue"
    
    def render(self, question: str, params: Dict) -> str:
        q = question.lower()
        entities = params.get("entities", {})
        
        # Kiểm tra xem có phải preview mode không (xem dữ liệu raw)
        is_preview = any(kw in q for kw in ["xem dữ liệu", "dữ liệu dự báo", "bảng dự báo", "table forecast",
                                            "dữ liệu đã dự báo", "dữ liệu bạn đã dự báo", "dữ liệu được dự báo",
                                            "dữ liệu", "bảng", "table", "chi tiết", "xem"])
        
        table_name = self._detect_metric_table(question)
        
        # Preview mode: trả về raw rows
        if is_preview:
            # Vì dữ liệu Olist là historical (2017-2018), KHÔNG filter theo CURRENT_DATE
            # Luôn lấy tất cả dữ liệu forecast có sẵn, ưu tiên forecast_date mới nhất
            # Nếu user có nói "ngày tới", chỉ filter theo horizon (1-7), không filter date
            has_future_in_preview = any(kw in q for kw in ["ngày tới", "sắp tới", "tương lai", "sắp đến"])
            
            if has_future_in_preview:
                # User muốn xem forecast "ngày tới" → filter theo horizon 1-7
                horizon_days = self._extract_horizon_days(question, default=7)
                date_filter = f"WHERE horizon BETWEEN 1 AND {horizon_days}"
            else:
                # Preview mode: lấy tất cả dữ liệu forecast có sẵn (không filter date)
                # Ưu tiên hiển thị dữ liệu mới nhất (ORDER BY DESC)
                date_filter = ""
            
            sql = f"""
SELECT
    CAST(forecast_date AS DATE) AS forecast_date,
    horizon,
    product_id,
    region_id,
    yhat,
    yhat_lo,
    yhat_hi,
    target_type,
    model_name,
    run_id,
    CAST(generated_at AS DATE) AS generated_at
FROM {table_name}
{date_filter}
ORDER BY forecast_date DESC, product_id, region_id, horizon
LIMIT 200
"""
            return sql.strip()
        
        # Planning mode: aggregate với logic tất định (deterministic)
        horizon_days = self._extract_horizon_days(question, default=7)
        
        # Extract entities sử dụng logic cải thiện
        category = self._extract_category(question)
        region = self._extract_region(question)
        
        # Xác định GROUP BY logic:
        # - Nếu có category filter → GROUP BY category (và region nếu có)
        # - Nếu có region filter → GROUP BY region
        # - Nếu có "theo sản phẩm" → GROUP BY product_id
        # - Mặc định: GROUP BY region_id
        group_by_clauses = []
        select_clauses = [
            "SUM(f.yhat_lo) AS forecast_low",
            "SUM(f.yhat)    AS forecast_base",
            "SUM(f.yhat_hi) AS forecast_high",
            "COUNT(DISTINCT f.product_id) AS n_products",
            "COUNT(*) AS n_forecasts"
        ]
        joins = []
        post_join_filters = []  # Filters áp dụng sau JOIN (category)
        
        # FIX: Không filter theo latest.forecast_date vì có thể ngày đó không có horizon 1-7
        # Thay vào đó, lấy tất cả forecast có horizon 1-7 từ bất kỳ forecast_date nào
        # Ưu tiên forecast_date mới nhất bằng cách ORDER BY forecast_date DESC trong CTE
        filters = [f"f.horizon BETWEEN 1 AND {horizon_days}"]
        
        # Nếu user có nói "ngày tới" hoặc "sắp tới" → có thể muốn forecast gần nhất
        # Nhưng với dữ liệu historical (2018), ta vẫn lấy tất cả horizon 1-7 có sẵn
        has_future_kw = any(kw in q for kw in ["ngày tới", "sắp tới", "tương lai", "sắp đến"])
        # Với dữ liệu historical, ta không filter theo CURRENT_DATE, chỉ lấy horizon phù hợp
        
        # Xử lý Category (Logic cải thiện)
        if category:
            # Bắt buộc JOIN với dim_product và dim_product_category
            joins.append("LEFT JOIN lakehouse.gold.dim_product dp ON f.product_id = dp.product_id")
            joins.append("LEFT JOIN lakehouse.gold.dim_product_category dpc ON dp.product_category_name = dpc.product_category_name")
            
            # Category filter phải áp dụng SAU JOIN, không thể trong CTE
            post_join_filters.append(f"dpc.product_category_name_english = '{category}'")
            
            # GROUP BY category
            group_by_clauses.append("dpc.product_category_name_english")
            select_clauses.insert(0, "dpc.product_category_name_english AS category")
        
        # ✅ FIX: Bỏ tất cả default filters - chỉ filter khi user yêu cầu rõ ràng
        
        # Xử lý Region - CHỈ filter khi user nói rõ
        if region:
            filters.append(f"f.region_id = '{region}'")
            group_by_clauses.append("f.region_id")
            # ✅ FIX: Safe insert - insert before last 5 items, or at position 0 if less than 5 items
            insert_pos = max(0, len(select_clauses) - 5)
            select_clauses.insert(insert_pos, "f.region_id AS region_id")
        
        # GROUP BY logic - CHỈ dựa vào user request, KHÔNG có default
        wants_by_product = any(kw in q for kw in ["sản phẩm", "product", "series", "theo sản phẩm", "theo product"])
        wants_by_region = any(kw in q for kw in ["bang", "state", "region", "vùng", "theo bang", "theo region", "cho từng bang", "từng bang"])
        
        if wants_by_product:
            # User muốn group theo product
            group_by_clauses = ["f.product_id"]
            select_clauses.insert(0, "f.product_id AS product_id")
            if not joins:  # Chỉ JOIN nếu chưa có (vì category đã JOIN rồi)
                joins.append("LEFT JOIN lakehouse.gold.dim_product dp ON f.product_id = dp.product_id")
        elif wants_by_region or region:
            # User muốn group theo region (hoặc đã filter region)
            if "f.region_id" not in group_by_clauses:
                group_by_clauses.append("f.region_id")
                # ✅ FIX: Safe insert - insert before last 5 items, or at position 0 if less than 5 items
                # Chỉ insert nếu chưa có trong select_clauses
                if not any("f.region_id AS region_id" in clause for clause in select_clauses):
                    insert_pos = max(0, len(select_clauses) - 5)
                    select_clauses.insert(insert_pos, "f.region_id AS region_id")
        # ✅ REMOVED: Không còn default GROUP BY region_id nếu không có group_by_clauses
        
        # Loại bỏ duplicate JOINs
        joins = list(dict.fromkeys(joins))  # Preserve order, remove duplicates
        
        # FIX: Thay vì filter theo latest.forecast_date (có thể không có horizon 1-7),
        # ta lấy tất cả forecast có horizon phù hợp và ưu tiên forecast_date mới nhất
        # Sử dụng ROW_NUMBER để chọn forecast_date mới nhất cho mỗi product_id × region_id
        where_clause = "WHERE f.rn = 1"  # Chỉ lấy forecast_date mới nhất
        if post_join_filters:
            where_clause += " AND " + " AND ".join(post_join_filters)
        
        # ✅ FIX: Build SQL với format chuẩn, đảm bảo GROUP BY luôn có khi cần
        # Nếu group_by_clauses rỗng nhưng có aggregation → lỗi SQL, nên cần đảm bảo có GROUP BY
        if not group_by_clauses and any('SUM(' in s or 'COUNT(' in s for s in select_clauses):
            # Có aggregation nhưng không có GROUP BY → có thể là bug, nhưng tạm thời bỏ qua
            # (trong trường hợp này không nên xảy ra vì forecast planning luôn cần GROUP BY)
            pass
        
        # Build SQL
        joins_str = ' '.join(joins) if joins else ''
        group_by_str = f'GROUP BY {", ".join(group_by_clauses)}' if group_by_clauses else ''
        
        # Format SQL với line breaks rõ ràng
        sql_parts = [
            f"WITH forecast_data AS (",
            f"    SELECT",
            f"        f.*,",
            f"        ROW_NUMBER() OVER (",
            f"            PARTITION BY f.product_id, f.region_id, f.horizon",
            f"            ORDER BY CAST(f.forecast_date AS DATE) DESC",
            f"        ) AS rn",
            f"    FROM {table_name} f",
            f"    WHERE {' AND '.join(filters)}",
            f")",
            f"SELECT",
            f"    {', '.join(select_clauses)}",
            f"FROM forecast_data f"
        ]
        
        if joins_str:
            sql_parts.append(joins_str)
        
        sql_parts.append(where_clause)
        
        if group_by_str:
            sql_parts.append(group_by_str)
        
        sql_parts.append("ORDER BY forecast_base DESC")
        sql_parts.append("LIMIT 50")
        
        sql = '\n'.join(sql_parts)
        return sql.strip()

