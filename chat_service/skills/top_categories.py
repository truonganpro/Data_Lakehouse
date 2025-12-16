# -*- coding: utf-8 -*-
"""Top categories skill - Top danh mục theo GMV / orders"""
from .base import BaseSkill
from typing import Dict


class TopCategoriesSkill(BaseSkill):
    """Top danh mục sản phẩm theo GMV hoặc số đơn"""
    
    def __init__(self):
        super().__init__()
        self.priority = 88  # Giữa RevenueTimeseries (90) và TopProducts (85)
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        
        # Must have "top" or ranking keywords
        has_top = any(
            kw in q
            for kw in [
                "top",
                "cao nhất",
                "nhiều nhất",
                "xếp hạng",
                "ranking",
                "top 10",
                "top 5",
                "top 20",
                "đứng đầu",
                "hàng đầu",
            ]
        )
        
        # Must have "danh mục" or "category"
        has_category = any(
            kw in q
            for kw in [
                "danh mục",
                "category",
                "nhóm sản phẩm",
                "loại sản phẩm",
                "nhóm hàng",
            ]
        )
        
        # Optional: revenue/GMV keywords (boost score if present)
        has_revenue = any(
            kw in q
            for kw in ["gmv", "doanh thu", "revenue", "tổng tiền"]
        )
        
        # Highest priority: Top + Category + Revenue/GMV
        # Score 0.99 to beat RevenueTimeseriesSkill (0.95)
        if has_top and has_category and has_revenue:
            return 0.99
        
        # High priority: Top + Category (even without explicit revenue, top implies ranking by value)
        if has_top and has_category:
            return 0.95
        
        return 0.0
    
    def render(self, question: str, params: Dict) -> str:
        start = params["time_window"]["start"]
        end = params["time_window"]["end"]
        topn = params.get("topn", 10)
        
        # Determine metric: GMV or orders
        q = question.lower()
        metric = (
            "gmv"
            if ("gmv" in q or "doanh thu" in q or "revenue" in q or "tổng tiền" in q)
            else "orders"
        )
        order_by = "gmv" if metric == "gmv" else "orders"
        
        sql = f"""
        WITH base AS (
            SELECT 
                pc.product_category_name_english AS category,
                SUM(i.price + i.freight_value) AS gmv,
                COUNT(DISTINCT i.order_id) AS orders,
                COUNT(*) AS units
            FROM lakehouse.gold.fact_order_item i
            LEFT JOIN lakehouse.gold.dim_product p 
                ON i.product_id = p.product_id
            LEFT JOIN lakehouse.gold.dim_product_category pc 
                ON p.product_category_name = pc.product_category_name
            WHERE i.full_date BETWEEN DATE '{start}' AND DATE '{end}'
              AND i.full_date IS NOT NULL
            GROUP BY 1
        )
        SELECT 
            category,
            gmv,
            orders,
            units,
            ROUND(gmv / NULLIF(orders, 0), 2) AS aov
        FROM base
        ORDER BY {order_by} DESC
        LIMIT {topn}
        """
        
        return sql.strip()

