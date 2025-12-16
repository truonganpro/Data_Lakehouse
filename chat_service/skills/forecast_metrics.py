# -*- coding: utf-8 -*-
"""Forecast metrics skill - sMAPE / MAE / RMSE / backtest"""

from .base import BaseSkill
from typing import Dict
import re


class ForecastMetricsSkill(BaseSkill):
    """
    Độ chính xác dự báo: sMAPE, MAE, RMSE từ platinum.forecast_monitoring.

    Ví dụ:
    - "sMAPE trung bình tháng vừa rồi của model forecast?"
    - "Độ chính xác dự báo 3 tháng gần đây?"
    """
    
    def __init__(self):
        super().__init__()
        self.priority = 94
    
    def match(self, question: str, entities: Dict) -> float:
        q = question.lower()
        has_metric_kw = any(
            kw in q
            for kw in ["smape", "mae", "rmse", "độ chính xác", "sai số", "backtest", "monitoring"]
        )
        if has_metric_kw:
            return 0.95
        return 0.0
    
    def _has_last_month(self, q: str) -> bool:
        return any(kw in q for kw in ["tháng vừa rồi", "tháng trước", "last month"])
    
    def render(self, question: str, params: Dict) -> str:
        q = question.lower()
        
        # Nếu user nói "tháng vừa rồi" → lấy tháng trước hiện tại
        if self._has_last_month(q):
            date_filter = """
WHERE date >= date_trunc('month', date_add('month', -1, CURRENT_DATE))
  AND date <  date_trunc('month', CURRENT_DATE)
"""
        else:
            # Mặc định lấy 3 tháng gần nhất
            date_filter = """
WHERE date >= date_add('month', -3, CURRENT_DATE)
"""
        
        # Nếu user chỉ định model cụ thể: "model abc123"
        model_filter = ""
        m_model = re.search(r"model\s+([a-zA-Z0-9_\-]+)", q)
        if m_model:
            model_name = m_model.group(1)
            model_filter = f"  AND model_name = '{model_name}'\n"
        
        sql = f"""
SELECT
    model_name,
    date_trunc('month', date) AS month,
    AVG(smape) AS avg_smape,
    AVG(abs_error) AS avg_mae,
    sqrt(AVG(abs_error * abs_error)) AS avg_rmse,
    COUNT(*) AS n_samples
FROM lakehouse.platinum.forecast_monitoring
{date_filter}{model_filter}
GROUP BY model_name, date_trunc('month', date)
ORDER BY month DESC, avg_smape ASC
LIMIT 50
"""
        return sql.strip()

