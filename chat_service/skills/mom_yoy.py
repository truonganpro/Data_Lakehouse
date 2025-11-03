# -*- coding: utf-8 -*-
"""Month-over-month / Year-over-year growth skill"""
from .base import BaseSkill
from typing import Dict


class MoMYoYSkill(BaseSkill):
    """Tăng trưởng MoM / YoY (so với kỳ trước)"""
    
    def __init__(self):
        super().__init__()
        self.priority = 75
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have growth/comparison keywords
        has_growth = any(kw in q for kw in [
            'tăng trưởng', 'growth', 'mom', 'yoy', 'so với', 
            'compare', 'so sánh', 'kỳ trước', 'previous', 'last'
        ])
        
        if has_growth:
            return 0.85
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        
        sql = f"""
        WITH monthly AS (
            SELECT 
                date_trunc('month', f.full_date) AS month,
                SUM(f.sum_price + f.sum_freight) AS gmv,
                COUNT(DISTINCT f.order_id) AS orders
            FROM lakehouse.gold.factorder f
            WHERE f.full_date BETWEEN DATE '{start}' AND DATE '{end}'
              AND f.full_date IS NOT NULL
            GROUP BY 1
        )
        SELECT 
            month,
            gmv,
            orders,
            gmv - LAG(gmv, 1) OVER (ORDER BY month) AS mom_gmv_abs,
            ROUND(100.0 * (gmv / NULLIF(LAG(gmv, 1) OVER (ORDER BY month), 0) - 1), 2) AS mom_gmv_pct,
            orders - LAG(orders, 1) OVER (ORDER BY month) AS mom_orders_abs,
            ROUND(100.0 * (CAST(orders AS DOUBLE) / NULLIF(LAG(orders, 1) OVER (ORDER BY month), 0) - 1), 2) AS mom_orders_pct
        FROM monthly
        ORDER BY month DESC
        LIMIT 24
        """
        
        return sql.strip()

