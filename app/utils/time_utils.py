# -*- coding: utf-8 -*-
"""
Time Utilities for Query Window
Chuẩn hóa xử lý thời gian, dual predicates, và time grain expressions
"""
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Tuple, Optional


def month_bounds(month_str: str) -> Tuple[date, date]:
    """
    Tính biên tháng (half-open interval)
    
    Args:
        month_str: 'YYYY-MM'
    
    Returns:
        (start_date, end_next_date) - end_next là đầu tháng kế tiếp
    """
    yyyy, mm = map(int, month_str.split("-"))
    start = date(yyyy, mm, 1)
    # end_next: đầu tháng kế tiếp
    if mm == 12:
        end_next = date(yyyy + 1, 1, 1)
    else:
        end_next = date(yyyy, mm + 1, 1)
    return start, end_next


def half_open_range(start: date, end: date) -> Tuple[date, date]:
    """
    Chuẩn hóa khoảng thời gian thành half-open interval [start, end_next)
    
    Args:
        start: Ngày bắt đầu (inclusive)
        end: Ngày kết thúc (inclusive trong UI, nhưng sẽ chuyển thành exclusive)
    
    Returns:
        (start, end_next) - end_next là ngày kế tiếp sau end
    """
    end_next = end + timedelta(days=1)
    return start, end_next


def dual_predicates(
    col_year_month_varchar: str,
    col_date: str,
    start: date,
    end_next: date
) -> Tuple[str, str]:
    """
    Sinh cặp điều kiện: một cho partition pruning, một cho đúng biên
    
    Args:
        col_year_month_varchar: Tên cột partition VARCHAR (ví dụ: 'year_month')
        col_date: Tên cột DATE chuẩn (ví dụ: 'full_date')
        start: Ngày bắt đầu
        end_next: Ngày kết thúc (exclusive)
    
    Returns:
        (prune_predicate, exact_predicate)
        - prune_predicate: Dùng để partition pruning (VARCHAR BETWEEN)
        - exact_predicate: Dùng để lọc đúng biên (DATE >= AND <)
    """
    # 1) Prune theo partition VARCHAR (YYYY-MM)
    ym_start = start.strftime("%Y-%m")
    # Tính year_month cuối cùng cần scan
    end_prev = end_next - timedelta(days=1)
    ym_end = end_prev.strftime("%Y-%m")
    
    prune = f"{col_year_month_varchar} BETWEEN '{ym_start}' AND '{ym_end}'"
    
    # 2) Đúng biên theo DATE half-open
    exact = f"{col_date} >= DATE '{start}' AND {col_date} < DATE '{end_next}'"
    
    return prune, exact


def timegrain_expr(grain: str, date_col: str, is_year_month: bool = False) -> str:
    """
    Sinh biểu thức time grain (date_trunc)
    
    Args:
        grain: 'day', 'week', 'month', 'quarter', 'year'
        date_col: Tên cột DATE
        is_year_month: True nếu date_col là VARCHAR 'YYYY-MM'
    
    Returns:
        Biểu thức SQL (ví dụ: "date_trunc('month', full_date)")
    """
    if is_year_month:
        # Parse year_month VARCHAR sang DATE trước
        month_date_expr = f"CAST(date_parse({date_col} || '-01', '%Y-%m-%d') AS date)"
        return f"date_trunc('{grain}', {month_date_expr})"
    else:
        return f"date_trunc('{grain}', {date_col})"


def coerce_date_col(table_meta: dict) -> Tuple[str, Optional[str]]:
    """
    Xác định cột ngày chuẩn và cột partition (nếu có)
    
    Args:
        table_meta: Metadata dict với 'date_col'
    
    Returns:
        (date_col, year_month_col) - year_month_col có thể là None
    """
    date_col = table_meta.get("date_col", "full_date")
    is_year_month = date_col in ["year_month"]
    
    if is_year_month:
        # Giả định bảng có cả year_month (VARCHAR) và full_date (DATE)
        # Nếu không có full_date, sẽ dùng parsed year_month
        return "full_date", date_col
    else:
        return date_col, None


def format_time_bucket_alias(grain: str) -> str:
    """
    Tên alias cho time bucket column
    
    Args:
        grain: 'day', 'week', 'month', etc.
    
    Returns:
        Tên alias (ví dụ: 'month_bucket')
    """
    return f"{grain}_bucket"

