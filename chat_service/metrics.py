# -*- coding: utf-8 -*-
"""
Metrics for Chat Service (Prometheus format)
"""
from typing import Dict, Optional
from datetime import datetime
from collections import defaultdict

from core.config import ENABLE_METRICS

# In-memory metrics store
_metrics: Dict[str, Dict] = defaultdict(lambda: {
    "count": 0,
    "total_duration_ms": 0,
    "total_rows": 0,
    "errors": 0
})


def record_request(route: str, status_code: int, duration_ms: int, rows: int = 0, error: bool = False):
    """
    Record request metrics
    
    Args:
        route: API route (e.g., "/ask", "/query")
        status_code: HTTP status code
        duration_ms: Request duration in milliseconds
        rows: Number of rows returned (for SQL queries)
        error: Whether the request resulted in an error
    """
    if not ENABLE_METRICS:
        return
    
    key = f"{route}_{status_code}"
    _metrics[key]["count"] += 1
    _metrics[key]["total_duration_ms"] += duration_ms
    _metrics[key]["total_rows"] += rows
    if error:
        _metrics[key]["errors"] += 1


def record_rate_limit_block():
    """Record rate limit block"""
    if not ENABLE_METRICS:
        return
    
    _metrics["rate_limit_block"]["count"] += 1


def get_metrics() -> Dict[str, Dict]:
    """
    Get current metrics in Prometheus format
    
    Returns:
        Dictionary of metrics
    """
    if not ENABLE_METRICS:
        return {}
    
    result = {}
    for key, values in _metrics.items():
        result[key] = {
            "count": values["count"],
            "avg_duration_ms": values["total_duration_ms"] / values["count"] if values["count"] > 0 else 0,
            "total_rows": values["total_rows"],
            "errors": values["errors"]
        }
    return result


def get_metrics_prometheus() -> str:
    """
    Get metrics in Prometheus format
    
    Returns:
        Prometheus metrics string
    """
    if not ENABLE_METRICS:
        return ""
    
    lines = []
    for key, values in _metrics.items():
        # Parse key (route_status_code)
        parts = key.split("_")
        route = "_".join(parts[:-1]) if len(parts) > 1 else key
        status_code = parts[-1] if len(parts) > 1 else "200"
        
        # Request count
        lines.append(f'chat_request_total{{route="{route}",status="{status_code}"}} {values["count"]}')
        
        # Average duration
        if values["count"] > 0:
            avg_duration = values["total_duration_ms"] / values["count"]
            lines.append(f'chat_latency_ms{{route="{route}",status="{status_code}"}} {avg_duration}')
        
        # Total rows
        if values["total_rows"] > 0:
            lines.append(f'sql_rows_returned_total{{route="{route}"}} {values["total_rows"]}')
        
        # Errors
        if values["errors"] > 0:
            lines.append(f'chat_errors_total{{route="{route}",status="{status_code}"}} {values["errors"]}')
    
    # Rate limit blocks
    if "rate_limit_block" in _metrics:
        lines.append(f'rate_limit_block_total {_metrics["rate_limit_block"]["count"]}')
    
    return "\n".join(lines)


def reset_metrics():
    """Reset all metrics"""
    _metrics.clear()

