# -*- coding: utf-8 -*-
"""
Vietnamese number parser for top-N, limits, etc.
"""
import re
from typing import Optional


# Vietnamese number words
VI_NUMBERS = {
    'một': 1, 'hai': 2, 'ba': 3, 'bốn': 4, 'năm': 5,
    'sáu': 6, 'bảy': 7, 'tám': 8, 'chín': 9, 'mười': 10,
    'mot': 1, 'bon': 4, 'nam': 5, 'sau': 6, 'bay': 7, 'tam': 8, 'chin': 9, 'muoi': 10,
    'mười lăm': 15, 'hai mươi': 20, 'ba mươi': 30, 'năm mươi': 50,
    'muoi lam': 15, 'hai muoi': 20, 'ba muoi': 30, 'nam muoi': 50,
}


def parse_topn(question: str, default: int = 10) -> int:
    """
    Parse top-N from question
    
    Args:
        question: User's question
        default: Default value if not found
        
    Returns:
        Integer N
    """
    q = question.lower()
    
    # Direct number: "top 5", "top 10"
    match = re.search(r'top\s+(\d+)', q)
    if match:
        return min(int(match.group(1)), 100)  # Cap at 100
    
    # Vietnamese words: "top năm", "top mười"
    for word, num in VI_NUMBERS.items():
        if f'top {word}' in q or f'top{word}' in q:
            return num
    
    # "N đầu tiên", "N đầu"
    match = re.search(r'(\d+)\s*(đầu tiên|đầu|dau tien|dau)', q)
    if match:
        return min(int(match.group(1)), 100)
    
    # Default
    return default


def parse_limit(question: str, default: int = 200) -> int:
    """
    Parse LIMIT value from question
    
    Returns:
        Integer limit (capped at 1000)
    """
    q = question.lower()
    
    # "50 đơn gần nhất", "100 records"
    match = re.search(r'(\d+)\s*(đơn|records?|rows?|dòng)', q)
    if match:
        return min(int(match.group(1)), 1000)
    
    return default


def normalize_number(text: str) -> str:
    """
    Convert Vietnamese number words to digits
    
    Example: "năm" -> "5", "mười" -> "10"
    """
    text_lower = text.lower()
    
    for word, num in VI_NUMBERS.items():
        text_lower = text_lower.replace(word, str(num))
    
    return text_lower


if __name__ == "__main__":
    # Test cases
    test_questions = [
        "Top 5 sản phẩm",
        "Top mười danh mục",
        "Top ba vùng miền",
        "50 đơn gần nhất",
        "Hiển thị 100 records",
        "Top sản phẩm",  # Should return default
    ]
    
    print("="*60)
    print("Testing Vietnamese Number Parser")
    print("="*60)
    
    for q in test_questions:
        topn = parse_topn(q)
        limit = parse_limit(q)
        print(f"\nQ: {q}")
        print(f"   Top-N: {topn}")
        print(f"   Limit: {limit}")

