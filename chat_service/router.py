# -*- coding: utf-8 -*-
"""
Intent router - chooses best skill for user question
"""
from typing import Dict, Optional, Tuple, List
import importlib
import os
from pathlib import Path

# Import NLP utilities
from nlp.vi_time import parse_time_window, parse_time_grain
from nlp.vi_numbers import parse_topn, parse_limit
from nlp.synonyms import extract_entities

# Import all skills
from skills.base import BaseSkill
from skills.revenue_timeseries import RevenueTimeseriesSkill
from skills.top_products import TopProductsSkill
from skills.distribution_region import DistributionRegionSkill
from skills.payment_mix import PaymentMixSkill
from skills.category_revenue import CategoryRevenueSkill
from skills.ontime_rate import OntimeRateSkill
from skills.mom_yoy import MoMYoYSkill
from skills.market_share import MarketShareSkill
from skills.aov_analysis import AOVAnalysisSkill
from skills.recent_orders import RecentOrdersSkill
from skills.top_sellers_sla import TopSellersSLASkill
from skills.cohort_retention import CohortRetentionSkill


class IntentRouter:
    """Routes user questions to appropriate SQL skills"""
    
    def __init__(self):
        self.skills: List[BaseSkill] = []
        self._load_skills()
    
    def _load_skills(self):
        """Load all available skills"""
        self.skills = [
            RevenueTimeseriesSkill(),
            TopProductsSkill(),
            DistributionRegionSkill(),
            PaymentMixSkill(),
            CategoryRevenueSkill(),
            OntimeRateSkill(),
            MoMYoYSkill(),
            MarketShareSkill(),
            AOVAnalysisSkill(),
            RecentOrdersSkill(),
            TopSellersSLASkill(),
            CohortRetentionSkill(),
        ]
        
        # Sort by priority
        self.skills.sort(key=lambda s: s.priority, reverse=True)
        
        print(f"✅ Loaded {len(self.skills)} skills")
    
    def parse_question(self, question: str) -> Dict:
        """
        Parse question to extract parameters
        
        Returns:
            Dict with time_window, time_grain, topn, limit, entities
        """
        return {
            'time_window': parse_time_window(question),
            'time_grain': parse_time_grain(question),
            'topn': parse_topn(question),
            'limit': parse_limit(question),
            'entities': extract_entities(question),
        }
    
    def route(self, question: str) -> Tuple[Optional[BaseSkill], Dict, float]:
        """
        Find best skill for question
        
        Args:
            question: User's question
            
        Returns:
            Tuple of (skill, params, confidence)
        """
        # Parse question
        params = self.parse_question(question)
        entities = params['entities']
        
        # Score all skills
        best_skill = None
        best_score = 0.0
        
        for skill in self.skills:
            try:
                score = skill.match(question, entities)
                
                if score > best_score:
                    best_score = score
                    best_skill = skill
            except Exception as e:
                print(f"⚠️  Error matching skill {skill.name}: {e}")
        
        return best_skill, params, best_score
    
    def generate_sql(self, question: str, threshold: float = 0.6) -> Tuple[Optional[str], Dict]:
        """
        Generate SQL for question using router
        
        Args:
            question: User's question
            threshold: Minimum confidence score (default 0.6)
            
        Returns:
            Tuple of (SQL string or None, metadata dict)
        """
        skill, params, confidence = self.route(question)
        
        metadata = {
            'skill_name': skill.name if skill else None,
            'confidence': confidence,
            'params': params,
        }
        
        if not skill or confidence < threshold:
            print(f"❌ No skill matched (best score: {confidence:.2f})")
            return None, metadata
        
        try:
            sql = skill.render(question, params)
            print(f"✅ Skill '{skill.name}' (confidence: {confidence:.2f})")
            return sql, metadata
        except Exception as e:
            print(f"❌ Error rendering SQL: {e}")
            return None, metadata
    
    def list_skills(self) -> List[Dict]:
        """Get list of available skills"""
        return [
            {
                'name': skill.name,
                'priority': skill.priority,
                'description': skill.__doc__ or '',
            }
            for skill in self.skills
        ]


# Global router instance
_router = None


def get_router() -> IntentRouter:
    """Get or create global router instance"""
    global _router
    if _router is None:
        _router = IntentRouter()
    return _router


if __name__ == "__main__":
    # Test router
    router = IntentRouter()
    
    test_questions = [
        "Doanh thu theo tháng 3 tháng gần đây?",
        "Top 10 sản phẩm bán chạy nhất?",
        "Phân bố đơn hàng theo vùng miền?",
        "Phương thức thanh toán nào phổ biến nhất?",
        "Tỷ lệ giao hàng đúng hạn theo tuần?",
        "So sánh doanh thu tháng này với tháng trước?",
        "Thị phần của từng danh mục?",
        "AOV theo phương thức thanh toán?",
        "50 đơn gần nhất?",
    ]
    
    print("="*60)
    print("Testing Intent Router")
    print("="*60)
    
    for q in test_questions:
        print(f"\nQ: {q}")
        sql, metadata = router.generate_sql(q)
        
        if sql:
            print(f"   Skill: {metadata['skill_name']}")
            print(f"   Confidence: {metadata['confidence']:.2f}")
            print(f"   SQL Preview: {sql[:150]}...")
        else:
            print(f"   No match (confidence: {metadata['confidence']:.2f})")

