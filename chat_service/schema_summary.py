# -*- coding: utf-8 -*-
"""
Schema Summary Builder
Builds schema summary text for LLM SQL generation prompt
"""
from typing import Dict, List, Optional
from about_dataset_provider import get_dataset_overview
import time

# Cache for schema summary (dataset doesn't change frequently)
_schema_summary_cache: Optional[str] = None
_cache_timestamp: Optional[float] = None
CACHE_TTL_SECONDS = 3600  # 1 hour cache


def build_schema_summary(include_details: bool = False, force_refresh: bool = False) -> str:
    """
    Build schema summary text to inject into SQL generation prompt.
    Uses cache to avoid rebuilding on every call (dataset is relatively static).
    
    Args:
        include_details: If True, include column details for key tables
        force_refresh: If True, bypass cache and rebuild
    
    Returns:
        Formatted schema summary string
    """
    global _schema_summary_cache, _cache_timestamp
    
    # Check cache (if not forcing refresh and cache is still valid)
    if not force_refresh and _schema_summary_cache and _cache_timestamp:
        age = time.time() - _cache_timestamp
        if age < CACHE_TTL_SECONDS:
            return _schema_summary_cache
    
    # Build fresh summary
    try:
        overview = get_dataset_overview()
    except Exception as e:
        print(f"⚠️  Error getting dataset overview: {e}")
        return _schema_summary_cache or ""  # Return cached if available, else empty
    
    lines = []
    lines.append("### DATASET SCHEMA OVERVIEW")
    lines.append("")
    
    # Gold Layer Tables
    lines.append("**Gold Layer (lakehouse.gold):**")
    gold_tables = overview.get("gold_tables", [])
    if gold_tables:
        for table in sorted(gold_tables):
            lines.append(f"  - `lakehouse.gold.{table}`")
        
        # Key tables with brief description
        lines.append("")
        lines.append("  Key tables:")
        key_gold_tables = {
            "fact_order": "Fact table chính - đơn hàng (full_date, order_id, customer_id, payment_total)",
            "fact_order_item": "Chi tiết items trong đơn (product_id, seller_id, price, freight_value)",
            "dim_product": "Dimension sản phẩm (product_id, product_category_name)",
            "dim_customer": "Dimension khách hàng (customer_id, customer_state)",
            "dim_seller": "Dimension người bán (seller_id, city_state)",
            "dim_product_category": "Dimension danh mục (product_category_name, product_category_name_english)",
        }
        
        for table, desc in key_gold_tables.items():
            if table in gold_tables:
                lines.append(f"    • `{table}`: {desc}")
    else:
        lines.append("  (No tables found)")
    
    lines.append("")
    
    # Platinum Layer Tables
    lines.append("**Platinum Layer (lakehouse.platinum):**")
    platinum_tables = overview.get("platinum_tables", [])
    if platinum_tables:
        for table in sorted(platinum_tables):
            lines.append(f"  - `lakehouse.platinum.{table}`")
        
        # Datamarts with descriptions
        lines.append("")
        lines.append("  Datamarts (pre-aggregated):")
        datamart_descriptions = {
            "dm_sales_monthly_category": "Doanh thu theo tháng/danh mục (year_month, gmv, orders, units, aov)",
            "dm_customer_lifecycle": "Phân tích customer lifecycle (cohort, retention metrics)",
            "dm_seller_kpi": "KPI người bán (GMV, orders, on_time_rate, avg_review_score)",
            "dm_logistics_sla": "SLA giao hàng (delivery_days_avg, on_time_rate)",
            "dm_payment_mix": "Phương thức thanh toán (payment_type, orders, payment_total)",
            "dm_product_bestsellers": "Sản phẩm bán chạy (product_id, gmv, units)",
            "dm_category_price_bands": "Phân khúc giá theo danh mục",
            "demand_forecast_revenue": "Dự báo doanh thu (forecast_date, region_id, horizon, yhat, yhat_lo, yhat_hi)",
            "demand_forecast_quantity": "Dự báo số lượng (forecast_date, region_id, horizon, yhat, yhat_lo, yhat_hi)",
            "forecast_monitoring": "Monitoring forecast accuracy (date, smape, mae, rmse)",
            "forecast_backtest_detail": "Chi tiết backtest forecast",
        }
        
        for table, desc in datamart_descriptions.items():
            if table in platinum_tables:
                lines.append(f"    • `{table}`: {desc}")
    else:
        lines.append("  (No tables found)")
    
    lines.append("")
    
    # Time Range
    time_range = overview.get("time_range", {})
    if time_range.get("min"):
        lines.append(f"**Data Time Range:** {time_range['min']} to {time_range.get('max', 'N/A')}")
        lines.append("")
    
    # Important Notes
    lines.append("**Important Notes:**")
    lines.append("  • Always use `lakehouse.gold.*` or `lakehouse.platinum.*` (full path)")
    lines.append("  • Prefer `platinum.dm_*` datamarts for aggregated queries (faster)")
    lines.append("  • Use `gold.fact_order` or `gold.fact_order_item` for detailed analysis")
    lines.append("  • Date column in gold tables: `full_date` (DATE type)")
    lines.append("  • Always add time filter when querying fact tables (WHERE full_date >= ...)")
    lines.append("  • Historical data (2016-2018), not real-time")
    
    # Build final summary
    summary = "\n".join(lines)
    
    # Update cache
    _schema_summary_cache = summary
    _cache_timestamp = time.time()
    
    return summary


def get_key_table_columns(schema: str, table: str) -> List[str]:
    """
    Get column list for a key table (for detailed schema info if needed)
    
    Args:
        schema: Schema name (gold or platinum)
        table: Table name
    
    Returns:
        List of column names
    """
    try:
        overview = get_dataset_overview()
        table_key = f"{schema}.{table}"
        table_info = overview.get("table_info", {}).get(table_key, {})
        return table_info.get("columns", [])
    except Exception as e:
        print(f"⚠️  Error getting columns for {schema}.{table}: {e}")
        return []


if __name__ == "__main__":
    # Test schema summary
    print("="*80)
    print("Testing Schema Summary")
    print("="*80)
    summary = build_schema_summary()
    print(summary)

