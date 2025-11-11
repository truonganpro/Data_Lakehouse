# -*- coding: utf-8 -*-
"""Top sellers with bad SLA skill"""
from .base import BaseSkill
from typing import Dict


class TopSellersSLASkill(BaseSkill):
    """Top seller theo SLA xấu (trễ hạn nhiều nhất)"""
    
    def __init__(self):
        super().__init__()
        self.priority = 75
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        has_seller = any(kw in q for kw in ['seller', 'người bán', 'nhà bán'])
        has_sla = any(kw in q for kw in ['sla', 'trễ hạn', 'late', 'bad', 'xấu', 'kém'])
        
        if has_seller and has_sla:
            return 0.9
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        topn = params.get('topn', 10)
        
        sql = f"""
        SELECT 
            s.seller_id,
            s.city_state AS region,
            ROUND(100.0 * SUM(CASE WHEN f.delivered_on_time = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS ontime_rate_pct,
            COUNT(DISTINCT f.order_id) AS orders,
            SUM(f.sum_price + f.sum_freight) AS revenue
        FROM lakehouse.gold.fact_order f
        JOIN lakehouse.gold.fact_order_item i ON f.order_id = i.order_id
        LEFT JOIN lakehouse.gold.dim_seller s ON i.seller_id = s.seller_id
        WHERE f.full_date BETWEEN DATE '{start}' AND DATE '{end}'
          AND f.full_date IS NOT NULL
        GROUP BY 1, 2
        HAVING orders >= 20
        ORDER BY ontime_rate_pct ASC
        LIMIT {topn}
        """
        
        return sql.strip()

