# -*- coding: utf-8 -*-
"""
SQL Guardrails - An toàn và kiểm tra SQL
Cải thiện: bỏ string literal, word boundary, auto-LIMIT thông minh
Dùng module chung với Chat Service
"""
import re
from typing import Optional, Tuple, List
from datetime import date

# Import common guardrails
try:
    from .sql_guardrails_common import (
        is_safe_select as _is_safe_select_common,
        check_multi_statement,
        find_outer_limit,
        add_outer_limit,
        auto_rewrite_cast_on_partition,
        check_grouping_sets_fallback,
        remove_sql_comments,
        remove_string_literals
    )
    COMMON_AVAILABLE = True
except ImportError:
    COMMON_AVAILABLE = False
    print("⚠️  sql_guardrails_common not available, using local implementation")


# Patterns for DDL/DML detection (word boundary) - fallback
DDL_PATTERN = r"\b(ALTER|DROP|TRUNCATE|CREATE|RENAME)\b"
DML_PATTERN = r"\b(INSERT|UPDATE|DELETE|MERGE)\b"


def is_safe_select(sql: str) -> bool:
    """
    Kiểm tra SQL có an toàn không (chỉ SELECT/WITH, không có DDL/DML)
    
    Cải thiện:
    - Dùng module chung (AST nếu có, fallback regex)
    - Bỏ nội dung trong string literal trước khi kiểm tra
    - Dùng word boundary để tránh false-positive với tên cột
    """
    if COMMON_AVAILABLE:
        safe, _ = _is_safe_select_common(sql, prefer_ast=True)
        return safe
    
    # Fallback: local implementation
    # 1) Bỏ nội dung nằm trong '...' và "..."
    no_strings = remove_string_literals(sql) if COMMON_AVAILABLE else re.sub(
        r"('([^']|\\')*'|\"([^\"]|\\\")*\")",
        "''",
        sql,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # 2) Chỉ cho WITH/SELECT
    has_only_select = re.match(
        r"^\s*(WITH\b|SELECT\b)",
        no_strings,
        flags=re.IGNORECASE
    ) is not None
    
    if not has_only_select:
        return False
    
    # 3) Chặn từ khóa DDL/DML (word boundary)
    blocked = re.search(
        DDL_PATTERN + "|" + DML_PATTERN,
        no_strings,
        flags=re.IGNORECASE
    ) is not None
    
    return not blocked


def maybe_add_limit(sql: str, default_limit: int = 10000, enabled: bool = True) -> str:
    """
    Tự động thêm LIMIT ở outer level nếu thiếu (không thêm nếu đã có LIMIT/OFFSET/FETCH)
    
    Args:
        sql: SQL query
        default_limit: Giới hạn mặc định
        enabled: Bật/tắt tính năng
    
    Returns:
        SQL với LIMIT (nếu cần)
    """
    if not enabled:
        return sql
    
    if COMMON_AVAILABLE:
        sql_with_limit, _ = add_outer_limit(sql, default_limit)
        return sql_with_limit
    
    # Fallback: simple check
    has_limit = re.search(
        r"\bLIMIT\b|\bOFFSET\b|\bFETCH\s+FIRST\b",
        sql,
        re.IGNORECASE
    )
    
    if has_limit:
        return sql
    
    return f"{sql.rstrip()}\nLIMIT {default_limit}"


def sql_in_list(col: str, values: list[str]) -> str:
    """
    Tạo SQL IN clause an toàn (chống SQL injection)
    
    Args:
        col: Tên cột
        values: Danh sách giá trị
    
    Returns:
        SQL IN clause (ví dụ: "col IN ('a','b')")
    """
    # Escape single quotes
    esc = [v.replace("'", "''") for v in values]
    quoted = [f"'{x}'" for x in esc]
    return f"{col} IN ({','.join(quoted)})"


def check_explain_analyze(sql: str) -> tuple[str, bool]:
    """
    Kiểm tra và chuyển EXPLAIN ANALYZE → EXPLAIN (an toàn hơn)
    
    Returns:
        (sql_modified, was_changed)
    """
    if re.search(r"\bEXPLAIN\s+ANALYZE\b", sql, re.IGNORECASE):
        sql_modified = re.sub(
            r"\bEXPLAIN\s+ANALYZE\b",
            "EXPLAIN",
            sql,
            flags=re.IGNORECASE
        )
        return sql_modified, True
    return sql, False


def check_order_by_with_limit(sql: str) -> tuple[bool, bool]:
    """
    Kiểm tra có LIMIT mà không có ORDER BY (cảnh báo)
    Dùng check_order_by_with_limit_outer để kiểm tra outer level
    
    Returns:
        (has_limit, has_order_by)
    """
    return check_order_by_with_limit_outer(sql)


def detect_cast_on_partition(sql: str, partition_col: str = "year_month") -> bool:
    """
    Phát hiện CAST trên cột partition (có thể mất partition pruning)
    
    Args:
        sql: SQL query
        partition_col: Tên cột partition (mặc định: year_month)
    
    Returns:
        True nếu phát hiện CAST trên partition column
    """
    pattern = rf"CAST\s*\(\s*{partition_col}[^)]*\)"
    return bool(re.search(pattern, sql, re.IGNORECASE))


def auto_fix_cast_on_partition(
    sql: str,
    partition_col: str = "year_month",
    date_col: str = "full_date",
    start: Optional[date] = None,
    end: Optional[date] = None
) -> Tuple[str, bool]:
    """
    Tự động sửa CAST trên partition → thêm dual predicates
    
    Args:
        sql: SQL query
        partition_col: Tên cột partition (VARCHAR)
        date_col: Tên cột DATE
        start: Ngày bắt đầu (nếu có)
        end: Ngày kết thúc (nếu có)
    
    Returns:
        (rewritten_sql, was_rewritten)
    """
    if COMMON_AVAILABLE:
        return auto_rewrite_cast_on_partition(sql, partition_col, date_col, start, end)
    
    # Fallback: chỉ phát hiện, không auto-fix
    if detect_cast_on_partition(sql, partition_col):
        return sql, False
    return sql, False


def check_multi_statement_safe(sql: str) -> Tuple[bool, Optional[str]]:
    """
    Kiểm tra multi-statement (chỉ cho phép 1 statement)
    
    Args:
        sql: SQL query
    
    Returns:
        (is_single_statement, error_if_multiple)
    """
    if COMMON_AVAILABLE:
        return check_multi_statement(sql)
    
    # Fallback: simple check
    sql_clean = sql.rstrip().rstrip(';')
    if sql_clean.count(';') > 0:
        return False, "Multiple statements detected"
    return True, None


def check_order_by_with_limit_outer(sql: str) -> Tuple[bool, bool]:
    """
    Kiểm tra có LIMIT outer mà không có ORDER BY outer (cảnh báo)
    
    Args:
        sql: SQL query
    
    Returns:
        (has_outer_limit, has_outer_order_by)
    """
    if COMMON_AVAILABLE:
        has_limit, _ = find_outer_limit(sql)
        # Simple check for ORDER BY (could be improved with AST)
        has_order_by = bool(re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE))
        return has_limit, has_order_by
    
    # Fallback
    has_limit = bool(re.search(r"\bLIMIT\b", sql, re.IGNORECASE))
    has_order_by = bool(re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE))
    return has_limit, has_order_by

