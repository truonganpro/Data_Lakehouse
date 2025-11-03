# -*- coding: utf-8 -*-
"""Average Order Value analysis skill"""
from .base import BaseSkill
from typing import Dict


class AOVAnalysisSkill(BaseSkill):
    """Giá trị đơn hàng trung bình (AOV) theo payment type / region / category"""
    
    def __init__(self):
        super().__init__()
        self.priority = 80
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have AOV keywords
        has_aov = any(kw in q for kw in [
            'aov', 'giá trị đơn hàng', 'giá trị trung bình',
            'average order', 'avg order', 'trung bình đơn'
        ])
        
        if has_aov:
            return 0.9
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        
        # Check dimension
        q = question.lower()
        
        if any(kw in q for kw in ['thanh toán', 'payment']):
            # AOV by payment type
            sql = f"""
            SELECT 
                f.primary_payment_type AS payment_type,
                ROUND(SUM(f.sum_price + f.sum_freight) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS aov,
                COUNT(DISTINCT f.order_id) AS orders,
                SUM(f.sum_price + f.sum_freight) AS total_revenue
            FROM lakehouse.gold.factorder f
            WHERE f.full_date BETWEEN DATE '{start}' AND DATE '{end}'
              AND f.full_date IS NOT NULL
            GROUP BY 1
            ORDER BY orders DESC
            LIMIT 20
            """
        elif any(kw in q for kw in ['vùng', 'miền', 'region']):
            # AOV by region
            sql = f"""
            SELECT 
                s.city_state AS region,
                ROUND(SUM(i.price + i.freight_value) / NULLIF(COUNT(DISTINCT i.order_id), 0), 2) AS aov,
                COUNT(DISTINCT i.order_id) AS orders,
                SUM(i.price + i.freight_value) AS total_revenue
            FROM lakehouse.gold.factorderitem i
            LEFT JOIN lakehouse.gold.dimseller s ON i.seller_id = s.seller_id
            WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
              AND i.full_date IS NOT NULL
            GROUP BY 1
            ORDER BY orders DESC
            LIMIT 30
            """
        else:
            # AOV by category
            sql = f"""
            SELECT 
                pc.product_category_name_english AS category,
                ROUND(SUM(i.price + i.freight_value) / NULLIF(COUNT(DISTINCT i.order_id), 0), 2) AS aov,
                COUNT(DISTINCT i.order_id) AS orders,
                SUM(i.price + i.freight_value) AS total_revenue
            FROM lakehouse.gold.factorderitem i
            LEFT JOIN lakehouse.gold.dimproduct p ON i.product_id = p.product_id
            LEFT JOIN lakehouse.gold.dimproductcategory pc 
              ON p.product_category_name = pc.product_category_name
            WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
              AND i.full_date IS NOT NULL
            GROUP BY 1
            ORDER BY orders DESC
            LIMIT 30
            """
        
        return sql.strip()

