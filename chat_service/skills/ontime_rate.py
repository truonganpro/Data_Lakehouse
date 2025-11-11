# -*- coding: utf-8 -*-
"""On-time delivery rate skill"""
from .base import BaseSkill
from typing import Dict


class OntimeRateSkill(BaseSkill):
    """Tỷ lệ giao hàng đúng hạn / trễ hạn"""
    
    def __init__(self):
        super().__init__()
        self.priority = 85
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have delivery/ontime keywords
        has_delivery = any(kw in q for kw in ['giao hàng', 'delivery', 'ship', 'đúng hạn', 'trễ hạn', 'ontime', 'late'])
        
        # And rate/percentage
        has_rate = any(kw in q for kw in ['tỷ lệ', 'rate', 'phần trăm', 'percent', '%'])
        
        if has_delivery and has_rate:
            return 0.95
        elif has_delivery:
            return 0.8
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        grain = params['time_grain']
        
        sql = f"""
        SELECT 
            date_trunc('{grain}', f.full_date) AS bucket,
            ROUND(100.0 * SUM(CASE WHEN f.delivered_on_time = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS ontime_rate_pct,
            ROUND(100.0 * SUM(CASE WHEN f.delivered_on_time = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS late_rate_pct,
            ROUND(100.0 * SUM(CASE WHEN f.is_canceled = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS cancel_rate_pct,
            COUNT(*) AS total_orders
        FROM lakehouse.gold.fact_order f
        WHERE f.full_date BETWEEN DATE '{start}' AND DATE '{end}'
          AND f.full_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 200
        """
        
        return sql.strip()

