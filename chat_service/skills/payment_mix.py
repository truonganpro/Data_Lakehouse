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
        
        # Payment-related keywords (including "phân bố phương thức thanh toán")
        has_payment = any(
            kw in q
            for kw in [
                'phương thức thanh toán',
                'thanh toán',
                'payment',
                'payment type',
                'trả tiền',
                'credit card',
                'boleto',
                'voucher',
                'debit',
                'debit card',
                'phân bố phương thức',
                'cơ cấu thanh toán',
            ]
        ) or ('phân bố' in q and 'thanh toán' in q)  # "phân bố phương thức thanh toán"
        
        # Metric keywords (orders, revenue, gmv, tỷ trọng)
        has_metric = any(
            kw in q
            for kw in [
                'số đơn',
                'orders',
                'tổng tiền',
                'gmv',
                'doanh thu',
                'revenue',
                'payment_total',
                'tỷ trọng',
                'từng phương thức',
            ]
        )
        
        # Time keywords (month, quarter, year, etc.)
        has_time = any(
            kw in q
            for kw in [
                'tháng',
                'month',
                '6 tháng',
                '6 months',
                'gần nhất',
                'quý',
                'quarter',
                'năm',
                'year',
                'theo tháng',
                'theo quý',
                'theo năm',
            ]
        )
        
        # Extra boost if asking for "popular" or "phổ biến"
        has_popular = any(kw in q for kw in ['phổ biến', 'popular', 'nhiều nhất', 'most'])
        
        # Highest priority: Payment + Metric + Time (e.g., "Phân bố phương thức thanh toán theo tháng")
        # Score 0.99 to beat RevenueTimeseriesSkill (0.95) and DistributionRegionSkill (0.97)
        if has_payment and has_metric and has_time:
            return 0.99
        
        # Also highest priority: Payment + Time (even without explicit metric, "phân bố theo tháng" implies metrics)
        # This handles "Phân bố phương thức thanh toán theo quý trong năm 2018"
        if has_payment and has_time:
            return 0.99
        
        # High priority: Payment + Metric (distribution of payment methods with metrics)
        if has_payment and has_metric:
            return 0.95
        
        # Medium priority: Payment + Popular
        if has_payment and has_popular:
            return 0.95
        
        # Standard: Just payment keywords
        if has_payment:
            return 0.75
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        
        # Parse year_month from start/end dates
        start_year_month = start[:7]  # YYYY-MM
        end_year_month = end[:7]      # YYYY-MM
        
        # Check if question asks for time-series breakdown (by month/quarter/year)
        q = question.lower()
        needs_time_series = any(
            kw in q
            for kw in [
                'theo tháng',
                'theo quý',
                'theo năm',
                'theo month',
                'theo quarter',
                'theo year',
                'phân bố theo tháng',
                'phân bố theo quý',
                'phân bố theo năm',
            ]
        )
        
        if needs_time_series:
            # Group by both year_month and payment_type for time-series analysis
            sql = f"""
            SELECT 
                pm.year_month AS dt,
                pm.payment_type,
                SUM(pm.orders) AS orders,
                SUM(pm.payment_total) AS total_revenue,
                SUM(pm.unique_customers) AS customers,
                ROUND(SUM(pm.payment_total) / NULLIF(SUM(pm.orders), 0), 2) AS avg_order_value
            FROM lakehouse.platinum.dm_payment_mix pm
            WHERE pm.year_month >= '{start_year_month}'
              AND pm.year_month <= '{end_year_month}'
            GROUP BY 1, 2
            ORDER BY 1 DESC, orders DESC
            LIMIT 200
            """
        else:
            # Aggregate across time period (total distribution)
            sql = f"""
            SELECT 
                pm.payment_type,
                SUM(pm.orders) AS total_orders,
                SUM(pm.unique_customers) AS total_customers,
                SUM(pm.payment_total) AS total_revenue,
                ROUND(SUM(pm.payment_total) / NULLIF(SUM(pm.orders), 0), 2) AS avg_order_value
            FROM lakehouse.platinum.dm_payment_mix pm
            WHERE pm.year_month >= '{start_year_month}'
              AND pm.year_month <= '{end_year_month}'
            GROUP BY 1
            ORDER BY total_orders DESC
            LIMIT 20
            """
        
        return sql.strip()

