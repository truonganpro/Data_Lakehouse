# -*- coding: utf-8 -*-
"""Regional distribution skill - phân bố theo vùng miền"""
from .base import BaseSkill
from typing import Dict


class DistributionRegionSkill(BaseSkill):
    """Phân bố đơn hàng/doanh thu theo vùng miền (seller location)"""
    
    def __init__(self):
        super().__init__()
        self.priority = 80
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have "phân bố" or "distribution" or "theo vùng/bang"
        has_distribution = any(
            kw in q
            for kw in [
                'phân bố',
                'distribution',
                'theo vùng',
                'theo miền',
                'theo bang',
                'by region',
                'by state',
            ]
        )
        
        # Or has "vùng miền" / "region" / "bang" / "state"
        has_region = any(
            kw in q
            for kw in [
                'vùng',
                'miền',
                'region',
                'khu vực',
                'tỉnh',
                'bang',
                'state',
                'geolocation_state',  # Explicit column name
            ]
        )
        
        # Check for metrics (doanh thu, đơn hàng, revenue, orders)
        has_metric = any(
            kw in q
            for kw in [
                'doanh thu',
                'revenue',
                'gmv',
                'đơn hàng',
                'orders',
                'tổng số đơn',
                'tổng đơn',
            ]
        )
        
        # Exclude payment-related questions (should go to PaymentMixSkill)
        has_payment = any(
            kw in q
            for kw in [
                'phương thức thanh toán',
                'thanh toán',
                'payment',
                'payment type',
            ]
        )
        if has_payment:
            return 0.0  # Let PaymentMixSkill handle it
        
        # Priority case: If question asks for distribution by region/bang WITH metrics
        # This should win over RevenueTimeseriesSkill (0.95)
        if (has_distribution or has_region) and has_metric:
            return 0.97  # Higher than RevenueTimeseriesSkill's 0.95
        
        # General case: just mentions region/distribution without explicit metric
        if has_distribution or has_region:
            return 0.85
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        
        sql = f"""
        SELECT 
            s.city_state AS region,
            COUNT(DISTINCT i.order_id) AS order_count,
            SUM(i.price + i.freight_value) AS gmv,
            ROUND(SUM(i.price + i.freight_value) / NULLIF(COUNT(DISTINCT i.order_id), 0), 2) AS aov,
            COUNT(DISTINCT i.product_id) AS unique_products
        FROM lakehouse.gold.fact_order_item i
        LEFT JOIN lakehouse.gold.dim_seller s ON i.seller_id = s.seller_id
        WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
          AND i.full_date IS NOT NULL
        GROUP BY 1
        ORDER BY order_count DESC
        LIMIT 50
        """
        
        return sql.strip()

