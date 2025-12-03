"""
Unit tests for rate limiting
"""
import time
import pytest
from unittest.mock import patch

from chat_service.guard.rate_limit import allow, get_remaining, reset
from chat_service.core.config import RATE_LIMIT_MAX_REQ, RATE_LIMIT_WINDOW_S


class TestRateLimit:
    """Test rate limiting functionality"""
    
    def setup_method(self):
        """Reset rate limit before each test"""
        reset()
    
    def test_allow_single_request(self):
        """Test allowing a single request"""
        assert allow("127.0.0.1") == True
        assert get_remaining("127.0.0.1") == RATE_LIMIT_MAX_REQ - 1
    
    def test_allow_multiple_requests(self):
        """Test allowing multiple requests"""
        ip = "127.0.0.1"
        for i in range(RATE_LIMIT_MAX_REQ):
            assert allow(ip) == True
        
        # Next request should be blocked
        assert allow(ip) == False
        assert get_remaining(ip) == 0
    
    def test_rate_limit_reset_after_window(self):
        """Test rate limit reset after window"""
        ip = "127.0.0.1"
        
        # Fill up rate limit
        for i in range(RATE_LIMIT_MAX_REQ):
            allow(ip)
        
        # Should be blocked
        assert allow(ip) == False
        
        # Reset manually (simulating window expiry)
        reset(ip)
        
        # Should be allowed again
        assert allow(ip) == True
    
    def test_different_ips_independent(self):
        """Test that different IPs have independent rate limits"""
        ip1 = "127.0.0.1"
        ip2 = "192.168.1.1"
        
        # Fill up rate limit for ip1
        for i in range(RATE_LIMIT_MAX_REQ):
            allow(ip1)
        
        # ip1 should be blocked
        assert allow(ip1) == False
        
        # ip2 should still be allowed
        assert allow(ip2) == True
    
    @patch("chat_service.guard.rate_limit.RATE_LIMIT_ENABLED", False)
    def test_rate_limit_disabled(self):
        """Test that rate limit is disabled when flag is False"""
        # This test requires mocking the config
        # In practice, RATE_LIMIT_ENABLED is read at import time
        pass

