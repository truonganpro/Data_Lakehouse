# -*- coding: utf-8 -*-
"""Payment mix skill - cơ cấu thanh toán"""
from .base import BaseSkill
from typing import Dict


class PaymentMixSkill(BaseSkill):
    """Phân tích phương thức thanh toán (sử dụng platinum.dm_payment_mix)"""
    
    def __init__(self):
        super().__init__()
        self.priority = 85
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have "thanh toán" or "payment"
        has_payment = any(kw in q for kw in ['thanh toán', 'payment', 'trả tiền', 'phương thức'])
        
        # Extra boost if asking for "popular" or "phổ biến"
        has_popular = any(kw in q for kw in ['phổ biến', 'popular', 'nhiều nhất', 'most'])
        
        if has_payment and has_popular:
            return 0.95
        elif has_payment:
            return 0.85
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        
        # Use platinum table for faster aggregation
        sql = f"""
        SELECT 
            pm.payment_type,
            SUM(pm.orders) AS total_orders,
            SUM(pm.unique_customers) AS total_customers,
            SUM(pm.payment_total) AS total_revenue,
            ROUND(SUM(pm.payment_total) / NULLIF(SUM(pm.orders), 0), 2) AS avg_order_value
        FROM lakehouse.platinum.dm_payment_mix pm
        WHERE pm.year_month BETWEEN date_format(DATE '{start}', '%Y-%m')
                                AND date_format(DATE '{end}', '%Y-%m')
        GROUP BY 1
        ORDER BY total_orders DESC
        LIMIT 20
        """
        
        return sql.strip()

