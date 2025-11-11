# -*- coding: utf-8 -*-
"""Market share skill - thị phần"""
from .base import BaseSkill
from typing import Dict


class MarketShareSkill(BaseSkill):
    """Thị phần danh mục / vùng trong tổng doanh thu"""
    
    def __init__(self):
        super().__init__()
        self.priority = 78
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have share/percentage keywords
        has_share = any(kw in q for kw in ['thị phần', 'market share', 'share', 'tỷ trọng', 'cơ cấu', 'phần trăm'])
        
        if has_share:
            return 0.9
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        
        # Check if asking for category or region share
        q = question.lower()
        is_region = any(kw in q for kw in ['vùng', 'miền', 'region', 'state'])
        
        if is_region:
            sql = f"""
            WITH base AS (
                SELECT 
                    s.city_state AS region,
                    SUM(i.price + i.freight_value) AS revenue
                FROM lakehouse.gold.fact_order_item i
                LEFT JOIN lakehouse.gold.dim_seller s ON i.seller_id = s.seller_id
                WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
                  AND i.full_date IS NOT NULL
                GROUP BY 1
            )
            SELECT 
                region,
                revenue,
                ROUND(100.0 * revenue / NULLIF(SUM(revenue) OVER(), 0), 2) AS share_pct,
                ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / NULLIF(SUM(revenue) OVER(), 0), 2) AS cumulative_pct
            FROM base
            ORDER BY revenue DESC
            LIMIT 30
            """
        else:
            # Category share
            sql = f"""
            WITH base AS (
                SELECT 
                    pc.product_category_name_english AS category,
                    SUM(i.price) AS revenue
                FROM lakehouse.gold.fact_order_item i
                LEFT JOIN lakehouse.gold.dim_product p ON i.product_id = p.product_id
                LEFT JOIN lakehouse.gold.dim_product_category pc 
                  ON p.product_category_name = pc.product_category_name
                WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
                  AND i.full_date IS NOT NULL
                GROUP BY 1
            )
            SELECT 
                category,
                revenue,
                ROUND(100.0 * revenue / NULLIF(SUM(revenue) OVER(), 0), 2) AS share_pct,
                ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / NULLIF(SUM(revenue) OVER(), 0), 2) AS cumulative_pct
            FROM base
            ORDER BY revenue DESC
            LIMIT 30
            """
        
        return sql.strip()

