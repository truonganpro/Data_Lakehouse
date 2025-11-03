# -*- coding: utf-8 -*-
"""Revenue time series skill - doanh thu theo thời gian"""
from .base import BaseSkill
from typing import Dict


class RevenueTimeseriesSkill(BaseSkill):
    """Doanh thu/GMV theo day/week/month"""
    
    def __init__(self):
        super().__init__()
        self.priority = 90
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have "doanh thu" or "revenue" or "gmv"
        has_revenue = any(kw in q for kw in ['doanh thu', 'revenue', 'gmv', 'bán được'])
        
        # And time dimension
        has_time = any(kw in q for kw in ['theo', 'theo thời gian', 'per', 'by', 'tháng', 'tuần', 'ngày', 'month', 'week', 'day'])
        
        if has_revenue and has_time:
            return 0.95
        elif has_revenue:
            return 0.7
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        grain = params['time_grain']
        
        sql = f"""
        SELECT 
            date_trunc('{grain}', f.full_date) AS dt,
            SUM(f.sum_price + f.sum_freight) AS gmv,
            COUNT(DISTINCT f.order_id) AS orders,
            COUNT(DISTINCT f.customer_id) AS customers,
            ROUND(SUM(f.sum_price + f.sum_freight) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS aov
        FROM lakehouse.gold.factorder f
        WHERE f.full_date BETWEEN DATE '{start}' AND DATE '{end}'
          AND f.full_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 200
        """
        
        return sql.strip()

