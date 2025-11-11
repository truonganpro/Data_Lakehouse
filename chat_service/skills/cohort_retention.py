# -*- coding: utf-8 -*-
"""Cohort retention skill - phân tích cohort và retention"""
from .base import BaseSkill
from typing import Dict
import re


class CohortRetentionSkill(BaseSkill):
    """Cohort & Retention analysis - heatmap và retention rate"""
    
    def __init__(self):
        super().__init__()
        self.priority = 88
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have cohort/retention keywords
        has_cohort = any(kw in q for kw in ['cohort', 'nhóm khách', 'nhóm mua'])
        has_retention = any(kw in q for kw in ['retention', 'giữ chân', 'tỷ lệ giữ', 'duy trì'])
        has_heatmap = any(kw in q for kw in ['heatmap', 'ma trận', 'bảng'])
        
        if (has_cohort or has_retention) and has_heatmap:
            return 0.95
        elif has_cohort or has_retention:
            return 0.9
        elif 'cohort' in q:
            return 0.85
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        # Extract cohort date range from question
        # Format: "2017-01 → 2017-06" or "2017-01 -> 2017-06" or "từ 2017-01 đến 2017-06"
        q = question.lower()
        
        # Try to extract YYYY-MM dates
        date_pattern = r'(\d{4})-(\d{2})'
        dates = re.findall(date_pattern, question)
        
        if len(dates) >= 2:
            # Use first two dates found
            y1, m1 = dates[0]
            y2, m2 = dates[1]
            cohort_start = f"{y1}-{m1}"
            cohort_end = f"{y2}-{m2}"
        elif len(dates) == 1:
            # Only one date - use it as start, end = start + 6 months
            y1, m1 = dates[0]
            cohort_start = f"{y1}-{m1}"
            # Add 6 months
            y_int = int(y1)
            m_int = int(m1)
            m_int += 6
            if m_int > 12:
                m_int -= 12
                y_int += 1
            cohort_end = f"{y_int:04d}-{m_int:02d}"
        else:
            # Default: use time_window if available
            time_window = params.get('time_window', {})
            start = time_window.get('start', '2017-01-01')
            end = time_window.get('end', '2018-01-01')
            # Extract YYYY-MM from dates
            cohort_start = start[:7] if len(start) >= 7 else '2017-01'
            cohort_end = end[:7] if len(end) >= 7 else '2018-01'
        
        # Extract max months_since_cohort (default 12)
        months_pattern = r'(\d+)\s*tháng'
        months_match = re.search(months_pattern, question)
        max_months = int(months_match.group(1)) if months_match else 12
        
        sql = f"""
        WITH first_purchase AS (
            SELECT 
                customer_id,
                MIN(d.year_month) as cohort_month
            FROM lakehouse.gold.fact_order fo
            JOIN lakehouse.gold.dim_date d ON fo.full_date = d.full_date
            WHERE fo.is_canceled = false
            GROUP BY customer_id
        ),
        cohort_size AS (
            SELECT 
                cohort_month,
                COUNT(DISTINCT customer_id) AS customers_in_cohort
            FROM first_purchase
            WHERE cohort_month >= '{cohort_start}' AND cohort_month < '{cohort_end}'
            GROUP BY cohort_month
        ),
        monthly_activity AS (
            SELECT 
                fp.cohort_month,
                d.year_month,
                COUNT(DISTINCT fo.customer_id) AS customers_active,
                COUNT(DISTINCT fo.order_id) AS total_orders,
                SUM(COALESCE(fo.sum_price, 0) + COALESCE(fo.sum_freight, 0)) AS total_gmv
            FROM lakehouse.gold.fact_order fo
            JOIN lakehouse.gold.dim_date d ON fo.full_date = d.full_date
            INNER JOIN first_purchase fp ON fo.customer_id = fp.customer_id
            WHERE fp.cohort_month >= '{cohort_start}' AND fp.cohort_month < '{cohort_end}'
              AND d.year_month >= fp.cohort_month
              AND fo.is_canceled = false
            GROUP BY fp.cohort_month, d.year_month
        )
        SELECT 
            ma.cohort_month,
            ma.year_month,
            CAST(SUBSTRING(ma.year_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.year_month, 6, 2) AS INTEGER) -
            (CAST(SUBSTRING(ma.cohort_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.cohort_month, 6, 2) AS INTEGER)) AS months_since_cohort,
            cs.customers_in_cohort,
            ma.customers_active,
            ROUND(100.0 * ma.customers_active / NULLIF(cs.customers_in_cohort, 0), 2) AS retention_pct,
            ma.total_orders,
            ROUND(ma.total_gmv, 2) AS total_gmv
        FROM monthly_activity ma
        JOIN cohort_size cs ON ma.cohort_month = cs.cohort_month
        WHERE CAST(SUBSTRING(ma.year_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.year_month, 6, 2) AS INTEGER) -
              (CAST(SUBSTRING(ma.cohort_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.cohort_month, 6, 2) AS INTEGER)) >= 0
          AND CAST(SUBSTRING(ma.year_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.year_month, 6, 2) AS INTEGER) -
              (CAST(SUBSTRING(ma.cohort_month, 1, 4) AS INTEGER) * 12 + CAST(SUBSTRING(ma.cohort_month, 6, 2) AS INTEGER)) <= {max_months}
        ORDER BY ma.cohort_month, months_since_cohort
        LIMIT 10000
        """
        
        return sql.strip()

