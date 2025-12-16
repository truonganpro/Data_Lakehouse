# -*- coding: utf-8 -*-
"""
Auto-fix utilities for SQL guardrails
Automatically fixes common SQL issues instead of rejecting queries
"""
import re
from typing import List, Optional


def ensure_limit(sql: str, default_limit: int = 1000) -> str:
    """
    Ensure SQL query has a LIMIT clause. If missing, add it.
    
    Args:
        sql: SQL query string
        default_limit: Default limit to add if missing (default: 1000)
    
    Returns:
        SQL query with LIMIT clause guaranteed
    """
    sql_upper = sql.upper().strip()
    
    # Check if LIMIT or OFFSET already exists
    if re.search(r"\bLIMIT\s+\d+\b", sql_upper) or re.search(r"\bOFFSET\s+\d+\b", sql_upper):
        return sql
    
    # Remove trailing semicolon if present
    sql_clean = sql.rstrip().rstrip(';').rstrip()
    
    # Add LIMIT
    return f"{sql_clean}\nLIMIT {default_limit}"


def add_default_time_filter(sql: str, months: int = 3) -> str:
    """
    Add default time filter for large fact tables if missing.
    
    This function adds a time filter (e.g., last 3 months) when querying
    large fact tables like fact_order or fact_order_item without a time predicate.
    
    Args:
        sql: SQL query string
        months: Number of months to look back (default: 3)
    
    Returns:
        SQL query with time filter added (if needed)
    """
    sql_lower = sql.lower()
    
    # Large fact tables that require time filter
    large_fact_tables = ["fact_order", "fact_order_item"]
    
    # Check if query touches large fact tables
    has_large_fact = any(f"lakehouse.gold.{table}" in sql_lower or f".{table}" in sql_lower 
                         for table in large_fact_tables)
    
    if not has_large_fact:
        return sql
    
    # Time columns to check
    time_columns = ["full_date", "year_month", "order_date"]
    
    # Check if any time column is already in WHERE clause
    if "where" in sql_lower:
        has_time_pred = any(
            col in sql_lower 
            for col in time_columns
        )
        if has_time_pred:
            return sql  # Already has time filter
    
    # Build default time filter
    # Use full_date as it's the standard time column in gold tables
    default_filter = f"full_date >= date_add('month', -{months}, CURRENT_DATE)"
    
    # Find WHERE clause position
    where_match = re.search(r"\bWHERE\b", sql_lower, re.IGNORECASE)
    
    if where_match:
        # WHERE clause exists - add AND condition at the end of WHERE clause
        # Find the end of WHERE clause (before GROUP BY, ORDER BY, LIMIT)
        where_pos = where_match.end()
        pattern = r"\b(?:GROUP BY|ORDER BY|LIMIT)\b"
        end_match = re.search(pattern, sql_lower[where_pos:], re.IGNORECASE)
        
        if end_match:
            # Insert AND condition before GROUP BY/ORDER BY/LIMIT
            insert_pos = where_pos + end_match.start()
            # Find the actual position in original SQL (account for case differences)
            before_clause = sql[:insert_pos].rstrip()
            sql = before_clause + f" AND {default_filter} " + sql[insert_pos:]
        else:
            # No GROUP BY/ORDER BY/LIMIT - add at end of WHERE clause
            sql = sql.rstrip().rstrip(';') + f" AND {default_filter}"
    else:
        # No WHERE clause - find FROM clause and add WHERE
        from_match = re.search(r"\bFROM\s+\w+", sql_lower, re.IGNORECASE)
        if not from_match:
            return sql  # Can't determine where to add WHERE
        
        # Find end of FROM clause (before WHERE, GROUP BY, ORDER BY, LIMIT)
        from_pos = from_match.end()
        pattern = r"\b(?:WHERE|GROUP BY|ORDER BY|LIMIT)\b"
        end_match = re.search(pattern, sql_lower[from_pos:], re.IGNORECASE)
        
        if end_match:
            insert_pos = from_pos + end_match.start()
            sql = (
                sql[:insert_pos] + 
                f" WHERE {default_filter}" + 
                sql[insert_pos:]
            )
        else:
            # Add at end (before any trailing semicolon)
            sql = sql.rstrip().rstrip(';') + f" WHERE {default_filter}"
    
    return sql


def auto_fix_sql_issues(sql: str, issues: List[str], default_limit: int = 1000) -> str:
    """
    Automatically fix multiple SQL issues in one pass.
    
    Args:
        sql: SQL query string
        issues: List of issue codes (e.g., ["MISSING_LIMIT", "MISSING_TIME_PRED"])
        default_limit: Default limit for LIMIT clause
    
    Returns:
        Fixed SQL query
    """
    fixed_sql = sql
    
    # Fix issues in order (time filter first, then LIMIT)
    if "MISSING_TIME_PRED" in issues:
        fixed_sql = add_default_time_filter(fixed_sql)
    
    if "MISSING_LIMIT" in issues:
        fixed_sql = ensure_limit(fixed_sql, default_limit)
    
    return fixed_sql


if __name__ == "__main__":
    # Test auto-fix functions
    print("="*60)
    print("Testing Auto-fix Functions")
    print("="*60)
    
    # Test 1: ensure_limit
    print("\n1. Testing ensure_limit():")
    test_sql1 = "SELECT * FROM lakehouse.gold.fact_order"
    fixed1 = ensure_limit(test_sql1, default_limit=500)
    print(f"Original: {test_sql1}")
    print(f"Fixed:    {fixed1}")
    
    # Test 2: add_default_time_filter
    print("\n2. Testing add_default_time_filter():")
    test_sql2 = "SELECT order_id, payment_total FROM lakehouse.gold.fact_order"
    fixed2 = add_default_time_filter(test_sql2, months=3)
    print(f"Original: {test_sql2}")
    print(f"Fixed:    {fixed2}")
    
    # Test 3: SQL with existing WHERE
    print("\n3. Testing add_default_time_filter() with existing WHERE:")
    test_sql3 = "SELECT * FROM lakehouse.gold.fact_order WHERE order_status = 'delivered'"
    fixed3 = add_default_time_filter(test_sql3, months=3)
    print(f"Original: {test_sql3}")
    print(f"Fixed:    {fixed3}")
    
    # Test 4: auto_fix_sql_issues
    print("\n4. Testing auto_fix_sql_issues():")
    test_sql4 = "SELECT order_id FROM lakehouse.gold.fact_order WHERE order_status = 'delivered'"
    fixed4 = auto_fix_sql_issues(test_sql4, ["MISSING_LIMIT", "MISSING_TIME_PRED"], default_limit=100)
    print(f"Original: {test_sql4}")
    print(f"Fixed:    {fixed4}")

