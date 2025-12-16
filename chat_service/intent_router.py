# -*- coding: utf-8 -*-
"""
Intent Router using Embeddings
Uses semantic similarity (cosine similarity) to detect user intent instead of keyword matching
"""
from typing import Dict, Optional, List
import numpy as np
from embeddings import embed_query

# Mapping intent name -> seed description text
# These descriptions capture the semantic meaning of each intent type
INTENT_SEEDS: Dict[str, str] = {
    "revenue": "doanh thu gmv sales revenue theo thời gian theo tháng theo tuần theo ngày tổng doanh số",
    "forecast": "dự báo forecast prediction nhu cầu future horizon yhat yhat_lo yhat_hi kịch bản scenario planning",
    "forecast_metrics": "so sánh model forecast độ chính xác smape mae rmse monitoring backtest accuracy model performance",
    "payment": "phương thức thanh toán payment mix credit card boleto voucher debit_card tỷ trọng thanh toán payment method",
    "sla": "giao hàng đúng hạn logistics vận chuyển on time late orders delivery days đúng hạn trễ hạn",
    "cohort": "cohort customer lifecycle retention churn khách hàng mới cũ tỷ lệ quay lại cohort analysis",
    "products": "sản phẩm product top bán chạy bestseller hàng đầu phổ biến nhất popular items top products",
    "category": "danh mục category loại sản phẩm theo danh mục category revenue theo category",
    "top_category": "top danh mục top category xếp hạng danh mục ranking categories top categories sản phẩm có GMV cao nhất theo danh mục",
    "region": "vùng miền bang region state phân bố địa lý theo vùng theo bang theo tiểu bang geographical distribution",
    "seller": "người bán seller nhà bán vendor performance kpi seller performance top seller",
    "aov": "aov average order value giá trị đơn hàng trung bình giá trị trung bình mỗi đơn",
    "mom_yoy": "so sánh tháng này tháng trước năm này năm trước mom yoy month over month year over year growth rate",
    "market_share": "thị phần market share phần trăm thị trường market dominance theo category",
    "recent": "gần đây mới nhất recent latest orders đơn hàng mới nhất recent orders",
    "about_forecast_metric": "smape mae rmse forecast metric forecast accuracy độ chính xác dự báo ci coverage yhat_lo yhat_hi monitoring backtest đánh giá chất lượng forecast metric nào tốt bao nhiêu phần trăm tốt",
}

# Cache for intent vectors (computed once at startup)
INTENT_VECS: Optional[Dict[str, List[float]]] = None


def _initialize_intent_vectors() -> Dict[str, List[float]]:
    """
    Initialize intent vectors by embedding seed descriptions.
    This is called once at module load or first use.
    """
    global INTENT_VECS
    
    if INTENT_VECS is not None:
        return INTENT_VECS
    
    print("📦 Initializing intent vectors (embedding seed descriptions)...")
    INTENT_VECS = {}
    
    try:
        for intent_name, seed_text in INTENT_SEEDS.items():
            vec = embed_query(seed_text)
            INTENT_VECS[intent_name] = vec
        print(f"✅ Initialized {len(INTENT_VECS)} intent vectors")
    except Exception as e:
        print(f"⚠️  Error initializing intent vectors: {e}")
        print("   Falling back to empty vectors (keyword matching will be used instead)")
        INTENT_VECS = {}
    
    return INTENT_VECS


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    Calculate cosine similarity between two vectors
    
    Args:
        vec1: First vector
        vec2: Second vector
    
    Returns:
        Cosine similarity score (-1.0 to 1.0, usually 0.0 to 1.0 for normalized embeddings)
    """
    try:
        v1 = np.array(vec1)
        v2 = np.array(vec2)
        
        # Normalize vectors
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        dot_product = np.dot(v1, v2)
        similarity = dot_product / (norm1 * norm2)
        
        return float(similarity)
    except Exception as e:
        print(f"⚠️  Error calculating cosine similarity: {e}")
        return 0.0


def detect_intent(question: str, threshold: float = 0.3) -> Optional[str]:
    """
    Detect user intent from question using embedding similarity
    
    Args:
        question: User's question
        threshold: Minimum similarity score to consider (default: 0.3)
    
    Returns:
        Intent name (e.g., "revenue", "forecast", "payment") or None if no match
    """
    # Initialize intent vectors if not already done
    intent_vecs = _initialize_intent_vectors()
    
    if not intent_vecs:
        # If vectors not available, return None (fallback to keyword matching)
        return None
    
    try:
        # Embed the user question
        q_vec = embed_query(question)
        
        # Find best matching intent
        best_intent = None
        best_score = -1.0
        
        for intent_name, intent_vec in intent_vecs.items():
            score = cosine_similarity(q_vec, intent_vec)
            
            if score > best_score:
                best_score = score
                best_intent = intent_name
        
        # Only return intent if score is above threshold
        if best_score >= threshold:
            print(f"🎯 Intent detected: '{best_intent}' (similarity: {best_score:.3f})")
            return best_intent
        else:
            print(f"⚠️  No intent matched (best score: {best_score:.3f} < threshold: {threshold})")
            return None
    
    except Exception as e:
        print(f"⚠️  Error detecting intent: {e}")
        return None


def detect_intent_with_scores(question: str) -> Dict[str, float]:
    """
    Detect intent and return all scores (for debugging/analysis)
    
    Args:
        question: User's question
    
    Returns:
        Dict mapping intent names to similarity scores
    """
    intent_vecs = _initialize_intent_vectors()
    
    if not intent_vecs:
        return {}
    
    try:
        q_vec = embed_query(question)
        scores = {}
        
        for intent_name, intent_vec in intent_vecs.items():
            score = cosine_similarity(q_vec, intent_vec)
            scores[intent_name] = score
        
        # Sort by score descending
        sorted_scores = dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))
        return sorted_scores
    
    except Exception as e:
        print(f"⚠️  Error detecting intent with scores: {e}")
        return {}


if __name__ == "__main__":
    # Test intent detection
    print("="*80)
    print("Testing Intent Router with Embeddings")
    print("="*80)
    
    test_questions = [
        "Doanh thu theo tháng 3 tháng gần đây?",
        "Dự báo doanh thu 7 ngày tới?",
        "Phương thức thanh toán nào phổ biến nhất?",
        "Tỷ lệ giao hàng đúng hạn theo tuần?",
        "Top 10 sản phẩm bán chạy nhất?",
        "So sánh doanh thu tháng này với tháng trước?",
        "sMAPE trung bình của model forecast?",
        "Thị phần của từng danh mục?",
    ]
    
    for q in test_questions:
        print(f"\n📝 Question: {q}")
        intent = detect_intent(q, threshold=0.3)
        
        if intent:
            print(f"   ✅ Intent: {intent}")
        else:
            print(f"   ❌ No intent matched")
        
        # Show top 3 scores
        scores = detect_intent_with_scores(q)
        top3 = list(scores.items())[:3]
        print(f"   Top 3: {', '.join([f'{name}({score:.3f})' for name, score in top3])}")

