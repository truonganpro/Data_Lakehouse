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
        
        # Check for time granularity keywords (more specific than generic "theo/by/per")
        # Explicit time grain keywords to avoid conflicts with "theo bang", "by region", etc.
        has_time_grain = any(
            kw in q
            for kw in [
                'theo tháng',
                'theo tuần',
                'theo ngày',
                'tháng',
                'tuần',
                'ngày',
                'month',
                'week',
                'day',
                'quý',
                'q',
                'quarter',
                'year',
                'năm',
                'theo thời gian',
            ]
        )
        
        # Check if question is about region/distribution (should NOT match)
        has_region_keywords = any(
            kw in q
            for kw in [
                'theo bang',
                'theo vùng',
                'theo miền',
                'by region',
                'by state',
                'phân bố',
                'distribution',
                'bang',
                'vùng',
                'region',
                'state',
                'geolocation_state',
            ]
        )
        
        # If question mentions region/distribution, don't match this skill
        if has_region_keywords:
            return 0.0
        
        # Match if has revenue and explicit time grain
        if has_revenue and has_time_grain:
            return 0.95
        elif has_revenue:
            return 0.7
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params['time_window']['start']
        end = params['time_window']['end']
        grain = params['time_grain']
        
        # For monthly queries, prefer platinum datamart (pre-aggregated, faster)
        if grain == 'month':
            # Use platinum.dm_sales_monthly_category for monthly aggregation
            # Parse year_month from start/end dates
            start_year_month = start[:7]  # YYYY-MM
            end_year_month = end[:7]      # YYYY-MM
            
            sql = f"""
            SELECT 
                year_month AS dt,
                SUM(gmv) AS gmv,
                SUM(orders) AS orders,
                SUM(units) AS units,
                ROUND(SUM(gmv) / NULLIF(SUM(orders), 0), 2) AS aov
            FROM lakehouse.platinum.dm_sales_monthly_category
            WHERE year_month >= '{start_year_month}'
              AND year_month <= '{end_year_month}'
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 200
            """
        else:
            # For day/week queries, use gold.fact_order
            # Clamp dates to Olist data range (2016-09-04 to 2018-10-17)
            start_clamped = max(start, '2016-09-04')
            end_clamped = min(end, '2018-10-17')
            
            sql = f"""
            SELECT 
                date_trunc('{grain}', f.full_date) AS dt,
                SUM(f.sum_price + f.sum_freight) AS gmv,
                COUNT(DISTINCT f.order_id) AS orders,
                COUNT(DISTINCT f.customer_id) AS customers,
                ROUND(SUM(f.sum_price + f.sum_freight) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS aov
            FROM lakehouse.gold.fact_order f
            WHERE f.full_date BETWEEN DATE '{start_clamped}' AND DATE '{end_clamped}'
              AND f.full_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 200
            """
        
        return sql.strip()

