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
        metric = 'revenue' if 'doanh thu' in q or 'revenue' in q else 'units'
        order_by = '3' if metric == 'revenue' else '4'  # Column position
        
        # Check for category filter
        category_filter = ""
        if 'category' in params.get('entities', {}):
            categories = params['entities']['category']
            cat_list = "', '".join(categories)
            category_filter = f"AND pc.product_category_name_english IN ('{cat_list}')"
        
        sql = f"""
        SELECT 
            pc.product_category_name_english AS category,
            i.product_id,
            SUM(i.price) AS revenue,
            COUNT(*) AS units,
            COUNT(DISTINCT i.order_id) AS orders
        FROM lakehouse.gold.factorderitem i
        LEFT JOIN lakehouse.gold.dimproduct p ON i.product_id = p.product_id
        LEFT JOIN lakehouse.gold.dimproductcategory pc 
          ON p.product_category_name = pc.product_category_name
        WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
          AND i.full_date IS NOT NULL
          {category_filter}
        GROUP BY 1, 2
        ORDER BY {order_by} DESC
        LIMIT {topn}
        """
        
        return sql.strip()

