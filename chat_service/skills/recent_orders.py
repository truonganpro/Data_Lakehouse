# -*- coding: utf-8 -*-
"""Recent orders skill - đơn hàng gần nhất"""
from .base import BaseSkill
from typing import Dict


class RecentOrdersSkill(BaseSkill):
    """N đơn hàng gần nhất"""
    
    def __init__(self):
        super().__init__()
        self.priority = 88
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have "gần nhất" or "recent" or "latest"
        has_recent = any(kw in q for kw in ['gần nhất', 'gần đây', 'recent', 'latest', 'mới nhất'])
        
        # And "đơn hàng" or "orders"
        has_orders = any(kw in q for kw in ['đơn hàng', 'đơn', 'orders', 'order'])
        
        if has_recent and has_orders:
            return 0.95
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        limit = params.get('limit', 50)
        
        sql = f"""
        SELECT 
            f.full_date,
            f.order_id,
            f.customer_id,
            f.primary_payment_type AS payment_type,
            f.items_count,
            f.sum_price + f.sum_freight AS gmv,
            f.delivered_on_time,
            f.is_canceled
        FROM lakehouse.gold.factorder f
        WHERE f.full_date IS NOT NULL
        ORDER BY f.full_date DESC
        LIMIT {limit}
        """
        
        return sql.strip()

