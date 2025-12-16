# -*- coding: utf-8 -*-
"""
Rate Limiting - Simple in-memory token bucket per IP
"""
import time
from collections import defaultdict, deque
from typing import Optional

from core.config import (
    RATE_LIMIT_ENABLED, RATE_LIMIT_WINDOW_S, RATE_LIMIT_MAX_REQ
)


# In-memory store: IP -> deque of timestamps
_hits: defaultdict[str, deque] = defaultdict(lambda: deque())


def allow(ip: str) -> bool:
    """
    Check if request from IP is allowed (rate limit)
    
    Args:
        ip: Client IP address
    
    Returns:
        True if allowed, False if rate limited
    """
    if not RATE_LIMIT_ENABLED:
        return True
    
    now = time.time()
    q = _hits[ip]
    
    # Remove timestamps older than window
    while q and now - q[0] > RATE_LIMIT_WINDOW_S:
        q.popleft()
    
    # Check if over limit
    if len(q) >= RATE_LIMIT_MAX_REQ:
        return False
    
    # Add current timestamp
    q.append(now)
    return True


def get_remaining(ip: str) -> int:
    """
    Get remaining requests for IP
    
    Args:
        ip: Client IP address
    
    Returns:
        Number of remaining requests in current window
    """
    if not RATE_LIMIT_ENABLED:
        return RATE_LIMIT_MAX_REQ
    
    now = time.time()
    q = _hits[ip]
    
    # Remove timestamps older than window
    while q and now - q[0] > RATE_LIMIT_WINDOW_S:
        q.popleft()
    
    return max(0, RATE_LIMIT_MAX_REQ - len(q))


def reset(ip: Optional[str] = None):
    """
    Reset rate limit for IP (or all IPs if None)
    
    Args:
        ip: Client IP address (None = reset all)
    """
    if ip:
        if ip in _hits:
            del _hits[ip]
    else:
        _hits.clear()

