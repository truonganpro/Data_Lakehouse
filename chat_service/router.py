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

# Import intent router for embedding-based detection
try:
    from intent_router import detect_intent
    INTENT_ROUTER_AVAILABLE = True
except ImportError:
    print("⚠️  Intent router not available, using keyword matching only")
    INTENT_ROUTER_AVAILABLE = False

# Import all skills
from skills.base import BaseSkill
from skills.forecast_planning import ForecastPlanningSkill
from skills.top_categories import TopCategoriesSkill
from skills.forecast_metrics import ForecastMetricsSkill
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
            # Forecast trước, để ưu tiên
            ForecastPlanningSkill(),
            ForecastMetricsSkill(),
            # Top ranking skills (higher priority)
            TopCategoriesSkill(),
            TopProductsSkill(),
            # Time series and distribution
            RevenueTimeseriesSkill(),
            DistributionRegionSkill(),
            PaymentMixSkill(),
            # Category and analysis
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
        q_lower = question.lower()
        
        # ✅ FIX: Improved Forecast Intent Routing - Priority-based matching
        # Forecast queries should be routed more precisely before other skills
        has_forecast_kw = any(kw in q_lower for kw in ["dự báo", "forecast", "horizon", "yhat", "planning"])
        
        if has_forecast_kw:
            # Forecast metrics (comparison, accuracy, monitoring)
            if any(kw in q_lower for kw in ["so sánh", "compare", "model", "smape", "mae", "rmse", "monitoring", "backtest", "độ chính xác"]):
                # Try ForecastMetricsSkill first
                forecast_metrics_skill = None
                for skill in self.skills:
                    if skill.name == "ForecastMetricsSkill":
                        forecast_metrics_skill = skill
                        break
                if forecast_metrics_skill:
                    score = forecast_metrics_skill.match(question, entities)
                    if score >= 0.6:
                        return forecast_metrics_skill, params, score
            
            # Forecast planning queries (revenue/quantity forecasts)
            # Check ForecastPlanningSkill with higher priority
            forecast_planning_skill = None
            for skill in self.skills:
                if skill.name == "ForecastPlanningSkill":
                    forecast_planning_skill = skill
                    break
            
            if forecast_planning_skill:
                score = forecast_planning_skill.match(question, entities)
                # Ensure forecast planning has high confidence if forecast keywords present
                if has_forecast_kw and score < 0.7:
                    # Boost score for forecast queries
                    score = max(score, 0.85)
                if score >= 0.6:
                    return forecast_planning_skill, params, score
        
        # Try embedding-based intent detection (if available)
        # Then fallback to keyword-based skill matching
        best_skill = None
        best_score = 0.0
        
        # Map intent names to skill names for routing
        intent_to_skill_map = {
            "revenue": "RevenueTimeseriesSkill",
            "forecast": "ForecastPlanningSkill",
            "forecast_metrics": "ForecastMetricsSkill",
            "payment": "PaymentMixSkill",
            "sla": "OntimeRateSkill",
            "cohort": "CohortRetentionSkill",
            "products": "TopProductsSkill",
            "category": "CategoryRevenueSkill",
            "top_category": "TopCategoriesSkill",
            "region": "DistributionRegionSkill",
            "seller": "TopSellersSLASkill",
            "aov": "AOVAnalysisSkill",
            "mom_yoy": "MoMYoYSkill",
            "market_share": "MarketShareSkill",
            "recent": "RecentOrdersSkill",
            "about_forecast_metric": None,  # Handled in main.py, not a skill
        }
        
        # Use embedding-based intent detection if available (as primary method)
        if INTENT_ROUTER_AVAILABLE:
            try:
                detected_intent = detect_intent(question, threshold=0.35)
                
                # Debug: Log top-3 intent scores for analysis
                try:
                    from intent_router import detect_intent_with_scores
                    scores = detect_intent_with_scores(question)
                    top3 = list(scores.items())[:3]
                    print(f"🔍 Intent scores (top 3): {', '.join([f'{name}({score:.3f})' for name, score in top3])}")
                except Exception as e:
                    pass  # Non-critical, skip if fails
                
                if detected_intent and detected_intent in intent_to_skill_map:
                    target_skill_name = intent_to_skill_map[detected_intent]
                    
                    # Find the matching skill
                    for skill in self.skills:
                        if skill.name == target_skill_name:
                            score = skill.match(question, entities)
                            # Boost score if intent matches (higher confidence from semantic similarity)
                            if score >= 0.5:
                                score = max(score, 0.75)  # Boost to at least 0.75
                            if score > best_score:
                                best_score = score
                                best_skill = skill
                            break
            except Exception as e:
                print(f"⚠️  Intent detection error: {e}, falling back to keyword matching")
        
        # Fallback: Score all skills using keyword matching
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

