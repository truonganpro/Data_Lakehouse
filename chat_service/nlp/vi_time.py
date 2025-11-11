# -*- coding: utf-8 -*-
"""
Vietnamese time parser for natural language date expressions
"""
import re
from datetime import datetime, timedelta
from typing import Dict, Optional


def parse_time_window(question: str) -> Dict[str, str]:
    """
    Parse Vietnamese time expressions into date range
    
    Args:
        question: User's question
        
    Returns:
        Dict with 'start' and 'end' dates (YYYY-MM-DD format)
    """
    q = question.lower()
    today = datetime.now().date()
    
    # Default: last 12 months (or all available data if question has "gần đây" without number)
    # For Olist data (2016-2018), we'll use data coverage from the dataset
    default_start = today - timedelta(days=365)
    default_end = today
    
    # Special case: "gần đây" or "gần nhất" without number -> use last 12 months
    # For Olist data (2016-2018), we'll use the latest available data
    if any(kw in q for kw in ['gần đây', 'gần nhất', 'recent', 'latest']) and not re.search(r'\d+\s*(tháng|thang|tuần|tuan|ngày|ngay)', q):
        # "gần đây" without number -> default to last 12 months
        # But for Olist data, use 2017-2018 range (latest available)
        # Check if question mentions Olist or Brazilian data, or if no year mentioned
        if not re.search(r'20\d{2}', q):
            # No year mentioned -> use Olist data range (2017-2018)
            return {'start': '2017-01-01', 'end': '2018-12-31'}
        default_start = today - timedelta(days=365)
        return {'start': default_start.strftime('%Y-%m-%d'), 'end': default_end.strftime('%Y-%m-%d')}
    
    # N months ago/recent (3 tháng gần đây, 6 tháng qua, etc.)
    match = re.search(r'(\d+)\s*(tháng|thang)\s*(gần đây|gần nhất|qua|trước)', q)
    if match:
        n_months = int(match.group(1))
        start = today - timedelta(days=n_months * 30)
        return {'start': start.strftime('%Y-%m-%d'), 'end': today.strftime('%Y-%m-%d')}
    
    # N weeks (2 tuần qua, 4 tuần gần đây)
    match = re.search(r'(\d+)\s*(tuần|tuan)\s*(gần đây|gần nhất|qua|trước)', q)
    if match:
        n_weeks = int(match.group(1))
        start = today - timedelta(weeks=n_weeks)
        return {'start': start.strftime('%Y-%m-%d'), 'end': today.strftime('%Y-%m-%d')}
    
    # N days (7 ngày qua, 30 ngày gần đây)
    match = re.search(r'(\d+)\s*(ngày|ngay)\s*(gần đây|gần nhất|qua|trước)', q)
    if match:
        n_days = int(match.group(1))
        start = today - timedelta(days=n_days)
        return {'start': start.strftime('%Y-%m-%d'), 'end': today.strftime('%Y-%m-%d')}
    
    # Last month (tháng trước, tháng vừa rồi)
    if any(kw in q for kw in ['tháng trước', 'thang truoc', 'tháng vừa rồi']):
        first_of_last_month = (today.replace(day=1) - timedelta(days=1))
        start = first_of_last_month.replace(day=1)
        end = first_of_last_month
        return {'start': start.strftime('%Y-%m-%d'), 'end': end.strftime('%Y-%m-%d')}
    
    # This month (tháng này, tháng hiện tại)
    if any(kw in q for kw in ['tháng này', 'thang nay', 'tháng hiện tại']):
        start = today.replace(day=1)
        return {'start': start.strftime('%Y-%m-%d'), 'end': today.strftime('%Y-%m-%d')}
    
    # Last week (tuần trước, tuần vừa rồi)
    if any(kw in q for kw in ['tuần trước', 'tuan truoc', 'tuần vừa rồi']):
        start = today - timedelta(days=today.weekday() + 7)  # Last Monday
        end = start + timedelta(days=6)  # Last Sunday
        return {'start': start.strftime('%Y-%m-%d'), 'end': end.strftime('%Y-%m-%d')}
    
    # This week (tuần này)
    if any(kw in q for kw in ['tuần này', 'tuan nay']):
        start = today - timedelta(days=today.weekday())  # This Monday
        return {'start': start.strftime('%Y-%m-%d'), 'end': today.strftime('%Y-%m-%d')}
    
    # Quarter (quý 1, quý 2, Q1, Q2, etc.)
    match = re.search(r'(quý|quy|q)\s*([1-4])', q)
    if match:
        quarter = int(match.group(2))
        year_match = re.search(r'(20\d{2})', q)
        year = int(year_match.group(1)) if year_match else today.year
        
        start_month = (quarter - 1) * 3 + 1
        end_month = quarter * 3
        
        start = datetime(year, start_month, 1).date()
        if end_month == 12:
            end = datetime(year, 12, 31).date()
        else:
            end = (datetime(year, end_month + 1, 1) - timedelta(days=1)).date()
        
        return {'start': start.strftime('%Y-%m-%d'), 'end': end.strftime('%Y-%m-%d')}
    
    # Year (năm 2023, năm ngoái)
    match = re.search(r'năm\s*(20\d{2})', q)
    if match:
        year = int(match.group(1))
        start = datetime(year, 1, 1).date()
        end = datetime(year, 12, 31).date()
        return {'start': start.strftime('%Y-%m-%d'), 'end': end.strftime('%Y-%m-%d')}
    
    if 'năm ngoái' in q or 'nam ngoai' in q:
        year = today.year - 1
        start = datetime(year, 1, 1).date()
        end = datetime(year, 12, 31).date()
        return {'start': start.strftime('%Y-%m-%d'), 'end': end.strftime('%Y-%m-%d')}
    
    if 'năm nay' in q or 'năm này' in q:
        start = datetime(today.year, 1, 1).date()
        return {'start': start.strftime('%Y-%m-%d'), 'end': today.strftime('%Y-%m-%d')}
    
    # Default fallback: For Olist data, use 2017-2018 range if no specific time mentioned
    # This prevents queries from scanning empty date ranges
    if not re.search(r'20\d{2}', q):
        # No year mentioned -> use Olist data range (2017-2018, latest available)
        return {'start': '2017-01-01', 'end': '2018-12-31'}
    
    return {'start': default_start.strftime('%Y-%m-%d'), 'end': default_end.strftime('%Y-%m-%d')}


def parse_time_grain(question: str) -> str:
    """
    Parse time grain/granularity from question
    
    Returns: 'day', 'week', 'month', 'quarter', 'year'
    """
    q = question.lower()
    
    if any(kw in q for kw in ['theo ngày', 'mỗi ngày', 'hàng ngày', 'daily', 'per day']):
        return 'day'
    
    if any(kw in q for kw in ['theo tuần', 'mỗi tuần', 'hàng tuần', 'weekly', 'per week']):
        return 'week'
    
    if any(kw in q for kw in ['theo tháng', 'mỗi tháng', 'hàng tháng', 'monthly', 'per month']):
        return 'month'
    
    if any(kw in q for kw in ['theo quý', 'mỗi quý', 'quarterly', 'per quarter']):
        return 'quarter'
    
    if any(kw in q for kw in ['theo năm', 'mỗi năm', 'hàng năm', 'yearly', 'per year']):
        return 'year'
    
    # Default based on time window
    time_window = parse_time_window(question)
    start = datetime.strptime(time_window['start'], '%Y-%m-%d')
    end = datetime.strptime(time_window['end'], '%Y-%m-%d')
    days = (end - start).days
    
    if days <= 31:
        return 'day'
    elif days <= 90:
        return 'week'
    else:
        return 'month'


if __name__ == "__main__":
    # Test cases
    test_questions = [
        "Doanh thu 3 tháng gần đây?",
        "Top sản phẩm 2 tuần qua?",
        "Phân bố theo vùng tháng trước?",
        "Revenue Q2 2024?",
        "Orders năm ngoái?",
        "Xu hướng theo tuần 6 tháng qua?",
    ]
    
    print("="*60)
    print("Testing Vietnamese Time Parser")
    print("="*60)
    
    for q in test_questions:
        window = parse_time_window(q)
        grain = parse_time_grain(q)
        print(f"\nQ: {q}")
        print(f"   Window: {window['start']} → {window['end']}")
        print(f"   Grain: {grain}")

