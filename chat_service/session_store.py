# -*- coding: utf-8 -*-
"""
Session Store - Lightweight context memory (no PII)
Stores user preferences and recent query context within a session
"""
from typing import Dict, Optional
from datetime import datetime, timedelta
import threading

# In-memory store (can be replaced with Redis later)
_session_store: Dict[str, Dict] = {}
_store_lock = threading.Lock()

# TTL: 2 hours
SESSION_TTL = 7200  # seconds


def get_session_context(session_id: str) -> Dict:
    """
    Get session context
    
    Returns:
        Dict with last_time_window, last_dimensions, last_measures, preferred_grain
    """
    with _store_lock:
        if session_id not in _session_store:
            return {}
        
        context = _session_store[session_id]
        
        # Check TTL
        if "created_at" in context:
            age = (datetime.now() - context["created_at"]).total_seconds()
            if age > SESSION_TTL:
                del _session_store[session_id]
                return {}
        
        # Return context without timestamp
        return {
            "last_time_window": context.get("last_time_window"),
            "last_dimensions": context.get("last_dimensions", []),
            "last_measures": context.get("last_measures", []),
            "preferred_grain": context.get("preferred_grain")
        }


def update_session_context(
    session_id: str,
    time_window: Optional[str] = None,
    dimensions: Optional[list] = None,
    measures: Optional[list] = None,
    grain: Optional[str] = None
):
    """
    Update session context
    
    Args:
        session_id: Session identifier
        time_window: Last time window used (e.g., "last_3_months", "2017-01-01_to_2018-12-31")
        dimensions: Last dimensions used (e.g., ["product_category_name_english", "state"])
        measures: Last measures used (e.g., ["revenue", "order_count"])
        grain: Preferred time grain (e.g., "month", "day", "week")
    """
    with _store_lock:
        if session_id not in _session_store:
            _session_store[session_id] = {
                "created_at": datetime.now()
            }
        
        context = _session_store[session_id]
        
        if time_window:
            context["last_time_window"] = time_window
        if dimensions:
            context["last_dimensions"] = dimensions
        if measures:
            context["last_measures"] = measures
        if grain:
            context["preferred_grain"] = grain
        
        # Update timestamp
        context["updated_at"] = datetime.now()


def clear_session(session_id: str):
    """Clear session context"""
    with _store_lock:
        if session_id in _session_store:
            del _session_store[session_id]


def cleanup_expired_sessions():
    """Remove expired sessions (call periodically)"""
    with _store_lock:
        now = datetime.now()
        expired = []
        
        for session_id, context in _session_store.items():
            if "created_at" in context:
                age = (now - context["created_at"]).total_seconds()
                if age > SESSION_TTL:
                    expired.append(session_id)
        
        for session_id in expired:
            del _session_store[session_id]
        
        return len(expired)


def get_session_stats() -> Dict:
    """Get statistics about active sessions"""
    with _store_lock:
        cleanup_expired_sessions()
        return {
            "active_sessions": len(_session_store),
            "total_sessions": len(_session_store)
        }


if __name__ == "__main__":
    # Test
    print("="*60)
    print("Testing Session Store")
    print("="*60)
    
    # Test session
    session_id = "test_session_123"
    
    # Update context
    update_session_context(
        session_id,
        time_window="last_3_months",
        dimensions=["product_category_name_english"],
        measures=["revenue", "order_count"],
        grain="month"
    )
    
    # Get context
    context = get_session_context(session_id)
    print(f"Context: {context}")
    
    # Stats
    stats = get_session_stats()
    print(f"Stats: {stats}")

