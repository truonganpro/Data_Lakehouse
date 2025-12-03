"""
Unit tests for quick actions
"""
import pytest
from chat_service.guard.quick_actions import suggest_actions, apply_patch
from chat_service.errors import GuardCode


class TestQuickActions:
    """Test quick actions functionality"""
    
    def test_suggest_actions_missing_limit(self):
        """Test suggesting actions for missing LIMIT"""
        sql = "SELECT * FROM gold.fact_order"
        issues = [GuardCode.MISSING_LIMIT]
        
        actions = suggest_actions(sql, issues)
        assert len(actions) > 0
        assert any(action["patch"] == "APPEND_LIMIT_1000" for action in actions)
    
    def test_suggest_actions_missing_time_pred(self):
        """Test suggesting actions for missing time predicate"""
        sql = "SELECT * FROM gold.fact_order"
        issues = [GuardCode.MISSING_TIME_PRED]
        
        actions = suggest_actions(sql, issues)
        assert len(actions) > 0
        assert any(action["patch"] == "ADD_LAST_3M_FILTER" for action in actions)
    
    def test_suggest_actions_star_projection(self):
        """Test suggesting actions for SELECT *"""
        sql = "SELECT * FROM gold.fact_order"
        issues = [GuardCode.STAR_PROJECTION]
        
        actions = suggest_actions(sql, issues)
        assert len(actions) > 0
        assert any(action["patch"] == "REWRITE_NO_STAR" for action in actions)
    
    def test_suggest_actions_disallowed_schema(self):
        """Test suggesting actions for disallowed schema"""
        sql = "SELECT * FROM bronze.raw_data"
        issues = [GuardCode.DISALLOWED_SCHEMA]
        
        actions = suggest_actions(sql, issues)
        assert len(actions) > 0
        assert any(action["patch"] == "SWITCH_TO_PLATINUM" for action in actions)
    
    def test_apply_patch_append_limit(self):
        """Test applying APPEND_LIMIT_1000 patch"""
        sql = "SELECT * FROM gold.fact_order"
        patched = apply_patch(sql, "APPEND_LIMIT_1000")
        assert "LIMIT 1000" in patched.upper()
    
    def test_apply_patch_append_limit_already_exists(self):
        """Test that APPEND_LIMIT_1000 doesn't add LIMIT if it already exists"""
        sql = "SELECT * FROM gold.fact_order LIMIT 100"
        patched = apply_patch(sql, "APPEND_LIMIT_1000")
        # Should not add another LIMIT
        assert patched.upper().count("LIMIT") == 1
    
    def test_apply_patch_add_last_3m_filter(self):
        """Test applying ADD_LAST_3M_FILTER patch"""
        sql = "SELECT * FROM gold.fact_order"
        patched = apply_patch(sql, "ADD_LAST_3M_FILTER")
        assert "full_date" in patched.lower()
        assert "date_add" in patched.lower()
    
    def test_apply_patch_rewrite_no_star(self):
        """Test applying REWRITE_NO_STAR patch"""
        sql = "SELECT * FROM gold.fact_order"
        patched = apply_patch(sql, "REWRITE_NO_STAR")
        assert "SELECT *" not in patched.upper()
        assert "SELECT" in patched.upper()
    
    def test_apply_patch_switch_to_platinum(self):
        """Test applying SWITCH_TO_PLATINUM patch"""
        sql = "SELECT * FROM gold.fact_order"
        patched = apply_patch(sql, "SWITCH_TO_PLATINUM")
        assert "platinum" in patched.lower()

