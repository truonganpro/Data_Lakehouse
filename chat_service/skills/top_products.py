# -*- coding: utf-8 -*-
"""Top products skill - sản phẩm bán chạy"""
from .base import BaseSkill
from typing import Dict


class TopProductsSkill(BaseSkill):
    """Top sản phẩm theo revenue hoặc units"""
    
    def __init__(self):
        super().__init__()
        self.priority = 85
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have "top" or "cao nhất" or "bán chạy"
        has_top = any(kw in q for kw in ['top', 'cao nhất', 'bán chạy', 'nhiều nhất', 'best'])
        
        # And "sản phẩm" or "product"
        has_product = any(kw in q for kw in ['sản phẩm', 'product', 'hàng hóa', 'mặt hàng'])
        
        if has_top and has_product:
            return 0.95
        elif has_product:
            return 0.5
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        topn = params.get('topn', 10)
        
        # Check if user wants by units or revenue
        q = question.lower()
        metric = 'revenue' if 'doanh thu' in q or 'revenue' in q or 'gmv' in q else 'units'
        order_by = 'gmv' if metric == 'revenue' else 'units'
        
        # Check for category filter
        category_filter = ""
        if 'category' in params.get('entities', {}):
            categories = params['entities']['category']
            cat_list = "', '".join(categories)
            category_filter = f"AND pc.product_category_name_english IN ('{cat_list}')"
        
        sql = f"""
        WITH item AS (
          SELECT
            oi.product_id,
            SUM(oi.price + oi.freight_value) AS gmv,
            COUNT(*) AS units,
            COUNT(DISTINCT oi.order_id) AS orders
          FROM lakehouse.gold.fact_order_item oi
          WHERE oi.full_date >= DATE '{start}' AND oi.full_date < DATE '{end}'
            AND oi.full_date IS NOT NULL
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
        WHERE 1=1
          {category_filter}
        ORDER BY {order_by} DESC
        LIMIT {topn}
        """
        
        return sql.strip()

