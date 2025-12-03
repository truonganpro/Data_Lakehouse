"""
Unit tests for SQL explanation
"""
import pytest
from chat_service.llm_summarize import _explain_sql_and_lineage, _parse_schema_from_sql


class TestExplain:
    """Test SQL explanation functionality"""
    
    def test_parse_schema_from_sql_platinum(self):
        """Test parsing schema from SQL with platinum"""
        sql = "SELECT * FROM platinum.dm_sales_monthly_category"
        schema = _parse_schema_from_sql(sql)
        assert schema == "platinum"
    
    def test_parse_schema_from_sql_gold(self):
        """Test parsing schema from SQL with gold"""
        sql = "SELECT * FROM gold.fact_order"
        schema = _parse_schema_from_sql(sql)
        assert schema == "gold"
    
    def test_parse_schema_from_sql_default(self):
        """Test parsing schema from SQL without schema (default to gold)"""
        sql = "SELECT 1"
        schema = _parse_schema_from_sql(sql)
        assert schema == "gold"
    
    def test_explain_sql_and_lineage_platinum(self):
        """Test explaining SQL with platinum schema"""
        sql = "SELECT category, SUM(gmv) FROM platinum.dm_sales_monthly_category GROUP BY 1"
        schema = "platinum"
        rows_preview = [{"category": "electronics", "sum": 1000}]
        
        explanation = _explain_sql_and_lineage(sql, schema, rows_preview)
        assert explanation is not None
        assert "platinum" in explanation.lower() or "dm_sales_monthly_category" in explanation.lower()
    
    def test_explain_sql_and_lineage_gold(self):
        """Test explaining SQL with gold schema"""
        sql = "SELECT customer_id, SUM(payment_total) FROM gold.fact_order GROUP BY 1"
        schema = "gold"
        rows_preview = [{"customer_id": "123", "sum": 500}]
        
        explanation = _explain_sql_and_lineage(sql, schema, rows_preview)
        assert explanation is not None
        assert "gold" in explanation.lower() or "fact_order" in explanation.lower()
    
    def test_explain_sql_and_lineage_empty(self):
        """Test explaining SQL with empty rows"""
        sql = "SELECT * FROM gold.fact_order LIMIT 10"
        schema = "gold"
        rows_preview = []
        
        explanation = _explain_sql_and_lineage(sql, schema, rows_preview)
        # Should still return explanation even with empty rows
        assert explanation is not None

