# -*- coding: utf-8 -*-
"""
Vietnamese synonyms and entity mapping
"""
from typing import Dict, Optional, List


# Synonyms mapping
SYNONYMS = {
    # Metrics
    'doanh thu': ['revenue', 'gmv', 'sales', 'bán được'],
    'đơn hàng': ['orders', 'đơn', 'order'],
    'sản phẩm': ['products', 'product', 'hàng hóa', 'mặt hàng'],
    'khách hàng': ['customers', 'customer', 'người mua'],
    'người bán': ['sellers', 'seller', 'nhà bán'],
    
    # Dimensions
    'vùng miền': ['region', 'khu vực', 'vùng', 'miền', 'địa phương'],
    'danh mục': ['category', 'loại', 'nhóm sản phẩm', 'phân loại'],
    'phương thức thanh toán': ['payment type', 'payment method', 'hình thức thanh toán'],
    
    # Time
    'tháng': ['month', 'thang'],
    'tuần': ['week', 'tuan'],
    'ngày': ['day', 'ngay'],
    'quý': ['quarter', 'quy'],
    'năm': ['year', 'nam'],
    
    # Aggregations
    'tổng': ['total', 'sum', 'tong'],
    'trung bình': ['average', 'avg', 'mean', 'tb'],
    'cao nhất': ['max', 'maximum', 'top', 'lớn nhất'],
    'thấp nhất': ['min', 'minimum', 'bottom', 'nhỏ nhất'],
    
    # Status
    'giao hàng': ['delivery', 'delivered', 'ship'],
    'đúng hạn': ['on time', 'ontime', 'punctual'],
    'trễ hạn': ['late', 'delayed'],
    'hủy': ['cancel', 'canceled', 'cancelled'],
}


def get_canonical_term(term: str) -> str:
    """
    Get canonical/standard term from synonym
    
    Args:
        term: Input term
        
    Returns:
        Canonical term (key in SYNONYMS dict)
    """
    term_lower = term.lower()
    
    # Direct match
    if term_lower in SYNONYMS:
        return term_lower
    
    # Search in synonyms
    for canonical, synonyms in SYNONYMS.items():
        if term_lower in synonyms or any(syn in term_lower for syn in synonyms):
            return canonical
    
    return term_lower


def extract_entities(question: str) -> Dict[str, List[str]]:
    """
    Extract entities (filters, dimensions) from question
    
    Returns:
        Dict mapping entity type to values
    """
    q = question.lower()
    entities = {}
    
    # Payment types
    payment_types = ['credit_card', 'boleto', 'voucher', 'debit_card']
    for pt in payment_types:
        pt_vi = pt.replace('_', ' ')
        if pt in q or pt_vi in q or (pt == 'credit_card' and 'thẻ tín dụng' in q):
            entities.setdefault('payment_type', []).append(pt)
    
    # Categories (common ones in Brazilian e-commerce)
    categories = [
        'electronics', 'furniture', 'toys', 'sports', 'health_beauty',
        'computers', 'home', 'fashion', 'books'
    ]
    for cat in categories:
        if cat in q:
            entities.setdefault('category', []).append(cat)
    
    # Vietnamese category names
    cat_vi_map = {
        'điện tử': 'electronics',
        'đồ chơi': 'toys',
        'thể thao': 'sports',
        'sức khỏe': 'health_beauty',
        'máy tính': 'computers',
        'nhà cửa': 'home',
        'thời trang': 'fashion',
        'sách': 'books',
    }
    for vi, en in cat_vi_map.items():
        if vi in q:
            entities.setdefault('category', []).append(en)
    
    # Regions (Brazilian states)
    states = ['SP', 'RJ', 'MG', 'RS', 'PR', 'BA', 'SC']
    for state in states:
        if state.lower() in q or f'bang {state.lower()}' in q:
            entities.setdefault('region', []).append(state)
    
    # Status
    if any(kw in q for kw in ['đúng hạn', 'on time', 'ontime']):
        entities['ontime'] = True
    
    if any(kw in q for kw in ['trễ hạn', 'late', 'delayed']):
        entities['ontime'] = False
    
    if any(kw in q for kw in ['hủy', 'cancel', 'cancelled']):
        entities['canceled'] = True
    
    return entities


def normalize_text(text: str) -> str:
    """
    Normalize Vietnamese text (remove diacritics for matching)
    """
    import unicodedata
    
    # Remove diacritics
    nfd = unicodedata.normalize('NFD', text)
    text_no_diacritics = ''.join(c for c in nfd if not unicodedata.combining(c))
    
    return text_no_diacritics.lower()


if __name__ == "__main__":
    # Test cases
    test_questions = [
        "Doanh thu theo vùng miền 3 tháng qua?",
        "Top sản phẩm trong danh mục điện tử?",
        "Phương thức thanh toán credit_card?",
        "Tỷ lệ giao hàng đúng hạn tại SP?",
        "Đơn hàng bị hủy theo tuần?",
    ]
    
    print("="*60)
    print("Testing Vietnamese Synonyms & Entity Extraction")
    print("="*60)
    
    for q in test_questions:
        entities = extract_entities(q)
        print(f"\nQ: {q}")
        print(f"   Entities: {entities}")
        
        # Test canonical terms
        for word in ['doanh thu', 'vùng miền', 'sản phẩm']:
            canonical = get_canonical_term(word)
            print(f"   '{word}' → '{canonical}'")

