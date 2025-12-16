# -*- coding: utf-8 -*-
"""
SQL Guardrails Common - Module dùng chung cho Query Window & Chat Service
Ưu tiên AST (sqlglot), fallback regex
"""
import re
from typing import Optional, Tuple, Set, List
from datetime import date, timedelta

# Try to import sqlglot for AST parsing
try:
    import sqlglot
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    sqlglot = None


# Patterns
READONLY_PATTERN = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
DDL_PATTERN = r"\b(ALTER|DROP|TRUNCATE|CREATE|RENAME)\b"
DML_PATTERN = r"\b(INSERT|UPDATE|DELETE|MERGE)\b"


def remove_sql_comments(sql: str) -> str:
    """
    Loại bỏ comment SQL (-- và /* */)
    
    Args:
        sql: SQL query
    
    Returns:
        SQL không có comment
    """
    # Remove -- comments (single line)
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    # Remove /* */ comments (multi-line)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql.strip()


def remove_string_literals(sql: str) -> str:
    """
    Bỏ nội dung trong string literal (để kiểm tra keyword an toàn)
    
    Args:
        sql: SQL query
    
    Returns:
        SQL với string literal được thay bằng ''
    """
    # Xử lý cả escaped quotes
    no_strings = re.sub(
        r"('([^']|\\')*'|\"([^\"]|\\\")*\")",
        "''",
        sql,
        flags=re.IGNORECASE | re.DOTALL
    )
    return no_strings


def is_safe_select_ast(sql: str) -> Tuple[bool, Optional[str]]:
    """
    Kiểm tra SQL an toàn bằng AST (sqlglot)
    
    Args:
        sql: SQL query
    
    Returns:
        (is_safe, error_message_if_unsafe)
    """
    if not SQLGLOT_AVAILABLE:
        return None, "sqlglot not available"
    
    try:
        parsed = sqlglot.parse_one(sql, dialect="trino")
        
        if not parsed:
            return False, "Could not parse SQL"
        
        # Check for dangerous operation types
        dangerous_types = []
        if hasattr(sqlglot.expressions, 'Delete'):
            dangerous_types.append(sqlglot.expressions.Delete)
        if hasattr(sqlglot.expressions, 'Drop'):
            dangerous_types.append(sqlglot.expressions.Drop)
        if hasattr(sqlglot.expressions, 'Update'):
            dangerous_types.append(sqlglot.expressions.Update)
        if hasattr(sqlglot.expressions, 'Insert'):
            dangerous_types.append(sqlglot.expressions.Insert)
        if hasattr(sqlglot.expressions, 'Create'):
            dangerous_types.append(sqlglot.expressions.Create)
        if hasattr(sqlglot.expressions, 'AlterTable'):
            dangerous_types.append(sqlglot.expressions.AlterTable)
        if hasattr(sqlglot.expressions, 'Truncate'):
            dangerous_types.append(sqlglot.expressions.Truncate)
        
        for dangerous_type in dangerous_types:
            if parsed.find(dangerous_type):
                return False, f"Dangerous operation: {dangerous_type.__name__}"
        
        # Check if it's SELECT or WITH
        if not isinstance(parsed, (sqlglot.expressions.Select, sqlglot.expressions.Query)):
            # Check if it starts with WITH
            if not isinstance(parsed, sqlglot.expressions.With):
                return False, "Only SELECT and WITH queries are allowed"
        
        return True, None
        
    except Exception as e:
        return None, f"AST parsing error: {e}"


def is_safe_select_regex(sql: str) -> bool:
    """
    Kiểm tra SQL an toàn bằng regex (fallback)
    
    Args:
        sql: SQL query
    
    Returns:
        True nếu an toàn
    """
    # 1) Bỏ comment và string literal
    sql_clean = remove_sql_comments(sql)
    sql_clean = remove_string_literals(sql_clean)
    
    # 2) Chỉ cho WITH/SELECT
    has_only_select = READONLY_PATTERN.match(sql_clean) is not None
    if not has_only_select:
        return False
    
    # 3) Chặn DDL/DML (word boundary)
    blocked = re.search(
        DDL_PATTERN + "|" + DML_PATTERN,
        sql_clean,
        flags=re.IGNORECASE
    ) is not None
    
    return not blocked


def is_safe_select(sql: str, prefer_ast: bool = True) -> Tuple[bool, Optional[str]]:
    """
    Kiểm tra SQL an toàn (ưu tiên AST, fallback regex)
    
    Args:
        sql: SQL query
        prefer_ast: Ưu tiên dùng AST nếu có
    
    Returns:
        (is_safe, error_message_if_unsafe)
    """
    if prefer_ast and SQLGLOT_AVAILABLE:
        safe, error = is_safe_select_ast(sql)
        if safe is not None:  # AST parsing succeeded
            return safe, error
    
    # Fallback to regex
    safe = is_safe_select_regex(sql)
    return safe, None if safe else "SQL contains dangerous keywords or is not SELECT/WITH"


def check_multi_statement(sql: str) -> Tuple[bool, Optional[str]]:
    """
    Kiểm tra multi-statement (chỉ cho phép 1 statement)
    
    Args:
        sql: SQL query
    
    Returns:
        (is_single_statement, error_if_multiple)
    """
    # Remove comments and string literals first
    sql_clean = remove_sql_comments(sql)
    sql_clean = remove_string_literals(sql_clean)
    
    # Count semicolons (not in string literals, already removed)
    semicolon_count = sql_clean.count(';')
    
    # Allow trailing semicolon
    sql_clean_stripped = sql_clean.rstrip().rstrip(';')
    if sql_clean_stripped.count(';') > 0:
        return False, "Multiple statements detected (use only one SELECT/WITH query)"
    
    return True, None


def find_outer_limit(sql: str) -> Tuple[bool, int]:
    """
    Tìm LIMIT ở outer level (không phải trong subquery)
    
    Args:
        sql: SQL query
    
    Returns:
        (has_outer_limit, limit_value)
    """
    if not SQLGLOT_AVAILABLE:
        # Fallback: simple regex check
        match = re.search(r"\bLIMIT\s+(\d+)\b", sql, re.IGNORECASE)
        if match:
            return True, int(match.group(1))
        return False, 0
    
    try:
        parsed = sqlglot.parse_one(sql, dialect="trino")
        if not parsed:
            return False, 0
        
        # Find outermost SELECT
        outermost_select = parsed
        while hasattr(outermost_select, 'this') and isinstance(outermost_select.this, sqlglot.expressions.Select):
            outermost_select = outermost_select.this
        
        # Check for LIMIT in outermost
        if hasattr(outermost_select, 'limit'):
            limit_expr = outermost_select.limit
            if limit_expr and hasattr(limit_expr, 'expression'):
                limit_val = limit_expr.expression
                if hasattr(limit_val, 'this'):
                    return True, int(limit_val.this)
                return True, int(limit_val)
        
        return False, 0
        
    except Exception:
        # Fallback
        match = re.search(r"\bLIMIT\s+(\d+)\b", sql, re.IGNORECASE)
        if match:
            return True, int(match.group(1))
        return False, 0


def add_outer_limit(sql: str, default_limit: int = 10000) -> Tuple[str, bool]:
    """
    Thêm LIMIT ở outer level (nếu thiếu)
    
    Args:
        sql: SQL query
        default_limit: Giới hạn mặc định
    
    Returns:
        (sql_with_limit, was_added)
    """
    has_limit, _ = find_outer_limit(sql)
    if has_limit:
        return sql, False
    
    # Check for OFFSET/FETCH FIRST
    has_offset = bool(re.search(r"\bOFFSET\b|\bFETCH\s+FIRST\b", sql, re.IGNORECASE))
    if has_offset:
        return sql, False
    
    # Add LIMIT at the end (after ORDER BY if exists)
    sql_clean = sql.rstrip().rstrip(';')
    
    # Try to find ORDER BY position
    order_by_match = re.search(r"\bORDER\s+BY\b", sql_clean, re.IGNORECASE)
    if order_by_match:
        # Insert LIMIT after ORDER BY clause
        # Simple approach: append at end
        sql_with_limit = f"{sql_clean}\nLIMIT {default_limit}"
    else:
        sql_with_limit = f"{sql_clean}\nLIMIT {default_limit}"
    
    return sql_with_limit, True


def auto_rewrite_cast_on_partition(
    sql: str,
    partition_col: str = "year_month",
    date_col: str = "full_date",
    start: Optional[date] = None,
    end: Optional[date] = None
) -> Tuple[str, bool]:
    """
    Tự động rewrite CAST trên partition → dual predicates
    
    Args:
        sql: SQL query
        partition_col: Tên cột partition (VARCHAR, ví dụ: 'year_month')
        date_col: Tên cột DATE (ví dụ: 'full_date')
        start: Ngày bắt đầu (nếu có)
        end: Ngày kết thúc (nếu có)
    
    Returns:
        (rewritten_sql, was_rewritten)
    """
    # Detect CAST on partition column
    cast_pattern = rf"CAST\s*\(\s*{partition_col}[^)]*\)"
    if not re.search(cast_pattern, sql, re.IGNORECASE):
        return sql, False
    
    # If we have start/end dates, add partition predicate
    if start and end:
        start_month = start.strftime("%Y-%m")
        end_month = end.strftime("%Y-%m")
        
        # Find WHERE clause and add partition predicate
        where_match = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
        if where_match:
            # Add partition predicate after WHERE
            where_pos = where_match.end()
            partition_pred = f" {partition_col} >= '{start_month}' AND {partition_col} <= '{end_month}' AND"
            sql_rewritten = sql[:where_pos] + partition_pred + sql[where_pos:]
            return sql_rewritten, True
    
    return sql, False


def check_grouping_sets_fallback(
    use_grouping_sets: bool,
    use_rollup: bool,
    dims: list
) -> Tuple[bool, bool]:
    """
    Fallback GROUPING SETS/ROLLUP nếu không có dimensions
    
    Args:
        use_grouping_sets: Có dùng GROUPING SETS
        use_rollup: Có dùng ROLLUP
        dims: Danh sách dimensions
    
    Returns:
        (use_grouping_sets_fixed, use_rollup_fixed)
    """
    if (use_grouping_sets or use_rollup) and not dims:
        return False, False
    return use_grouping_sets, use_rollup

