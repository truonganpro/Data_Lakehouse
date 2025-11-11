# -*- coding: utf-8 -*-
"""Category revenue skill - doanh thu theo danh mục"""
from .base import BaseSkill
from typing import Dict


class CategoryRevenueSkill(BaseSkill):
    """Doanh thu/đơn hàng theo danh mục sản phẩm"""
    
    def __init__(self):
        super().__init__()
        self.priority = 82
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have "danh mục" or "category" or "loại"
        has_category = any(kw in q for kw in ['danh mục', 'category', 'loại', 'nhóm', 'phân loại'])
        
        # And revenue/orders
        has_metric = any(kw in q for kw in ['doanh thu', 'revenue', 'đơn hàng', 'orders', 'bán'])
        
        if has_category and has_metric:
            return 0.9
        elif has_category:
            return 0.7
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        topn = params.get('topn', 20)
        
        sql = f"""
        SELECT 
            pc.product_category_name_english AS category,
            SUM(i.price) AS revenue,
            COUNT(DISTINCT i.order_id) AS orders,
            COUNT(*) AS units,
            ROUND(SUM(i.price) / NULLIF(COUNT(DISTINCT i.order_id), 0), 2) AS aov
        FROM lakehouse.gold.fact_order_item i
        LEFT JOIN lakehouse.gold.dim_product p ON i.product_id = p.product_id
        LEFT JOIN lakehouse.gold.dim_product_category pc 
          ON p.product_category_name = pc.product_category_name
        WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
          AND i.full_date IS NOT NULL
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT {topn}
        """
        
        return sql.strip()

