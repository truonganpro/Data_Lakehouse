# -*- coding: utf-8 -*-
"""
Quick Actions - Suggested actions for guardrails
"""
from typing import List, Dict, Optional

# Import GuardCode from errors module
try:
    from errors import GuardCode
except ImportError:
    # Fallback: define GuardCode locally if needed
        from enum import Enum
        class GuardCode(str, Enum):
            MISSING_LIMIT = "MISSING_LIMIT"
            MISSING_TIME_PRED = "MISSING_TIME_PRED"
            STAR_PROJECTION = "STAR_PROJECTION"
            DISALLOWED_SCHEMA = "DISALLOWED_SCHEMA"


def suggest_actions(sql: str, issues: List[GuardCode]) -> List[Dict]:
    """
    Generate quick actions based on detected issues
    
    Args:
        sql: SQL query string
        issues: List of guard codes (issues detected)
    
    Returns:
        List of action dictionaries with label and patch type
    """
    actions = []
    
    if GuardCode.MISSING_LIMIT in issues:
        actions.append({
            "label": "Thêm LIMIT 1000",
            "patch": "APPEND_LIMIT_1000",
            "description": "Thêm LIMIT 1000 vào cuối truy vấn"
        })
    
    if GuardCode.MISSING_TIME_PRED in issues:
        actions.append({
            "label": "Lọc 3 tháng gần đây",
            "patch": "ADD_LAST_3M_FILTER",
            "description": "Thêm điều kiện WHERE full_date >= date_add('month', -3, CURRENT_DATE)"
        })
    
    if GuardCode.STAR_PROJECTION in issues:
        actions.append({
            "label": "Thay SELECT * bằng cột",
            "patch": "REWRITE_NO_STAR",
            "description": "Thay SELECT * bằng các cột cụ thể (cần phân tích SQL)"
        })
    
    if GuardCode.DISALLOWED_SCHEMA in issues:
        actions.append({
            "label": "Chuyển sang platinum datamart",
            "patch": "SWITCH_TO_PLATINUM",
            "description": "Sử dụng datamart platinum thay vì gold fact tables"
        })
    
    return actions


def apply_patch(sql: str, patch_type: str, **kwargs) -> str:
    """
    Apply patch to SQL query
    
    Args:
        sql: SQL query string
        patch_type: Type of patch to apply
        **kwargs: Additional arguments for patch
    
    Returns:
        Patched SQL query
    """
    if patch_type == "APPEND_LIMIT_1000":
        # Check if LIMIT already exists
        if "LIMIT" not in sql.upper():
            return f"{sql.rstrip(';')}\nLIMIT 1000"
        return sql
    
    elif patch_type == "ADD_LAST_3M_FILTER":
        # Add time filter for last 3 months
        # This is a simplified version - actual implementation should parse SQL AST
        if "WHERE" not in sql.upper():
            # Add WHERE clause
            if "FROM" in sql.upper():
                parts = sql.upper().split("FROM")
                return f"{parts[0]}FROM {parts[1]} WHERE full_date >= date_add('month', -3, CURRENT_DATE)"
        else:
            # Add to existing WHERE clause
            if "WHERE" in sql.upper():
                where_pos = sql.upper().find("WHERE")
                where_clause = sql[where_pos:]
                if "full_date" not in where_clause:
                    return f"{sql[:where_pos]}WHERE {where_clause[5:]} AND full_date >= date_add('month', -3, CURRENT_DATE)"
        return sql
    
    elif patch_type == "REWRITE_NO_STAR":
        # This requires SQL parsing - simplified version
        # Replace SELECT * with common columns based on table
        if "SELECT *" in sql.upper():
            # This is a placeholder - actual implementation should use SQL AST
            return sql.replace("SELECT *", "SELECT order_id, customer_id, full_date, payment_total")
        return sql
    
    elif patch_type == "SWITCH_TO_PLATINUM":
        # Switch from gold to platinum datamart
        # This requires understanding the query intent - simplified version
        if "gold.fact_order" in sql.lower():
            return sql.replace("gold.fact_order", "platinum.dm_sales_monthly_category")
        return sql
    
    return sql
