"""
Unit tests for LLM registry
"""
import os
import pytest
from unittest.mock import Mock, patch

from chat_service.llm.registry import (
    LLMBase, GeminiLLM, OpenAILLM, NoopLLM,
    get_llm, generate_with_fallback
)


class TestLLMRegistry:
    """Test LLM registry functionality"""
    
    def test_noop_llm_always_available(self):
        """Test that NoopLLM is always available"""
        llm = NoopLLM()
        assert llm.is_available() == True
        assert llm.generate("test") == ""
    
    @patch.dict(os.environ, {"LLM_PROVIDER": "none"})
    def test_get_llm_none(self):
        """Test getting NoopLLM when provider is none"""
        llm = get_llm("sql")
        assert isinstance(llm, NoopLLM)
        assert llm.is_available() == True
    
    @patch.dict(os.environ, {"LLM_PROVIDER": "gemini", "GOOGLE_API_KEY": "test_key"})
    @patch("chat_service.llm.registry.genai")
    def test_get_llm_gemini(self, mock_genai):
        """Test getting GeminiLLM when provider is gemini"""
        mock_genai.configure = Mock()
        llm = get_llm("sql")
        assert isinstance(llm, GeminiLLM)
    
    @patch.dict(os.environ, {"LLM_PROVIDER": "openai", "OPENAI_API_KEY": "test_key"})
    @patch("chat_service.llm.registry.openai")
    def test_get_llm_openai(self, mock_openai):
        """Test getting OpenAILLM when provider is openai"""
        mock_openai.api_key = "test_key"
        llm = get_llm("sql")
        assert isinstance(llm, OpenAILLM)
    
    @patch.dict(os.environ, {"LLM_PROVIDER": "none"})
    def test_generate_with_fallback_noop(self):
        """Test generate_with_fallback with NoopLLM"""
        result = generate_with_fallback("test", kind="sql")
        assert result == ""
    
    @patch.dict(os.environ, {"LLM_PROVIDER": "gemini", "GOOGLE_API_KEY": "test_key"})
    @patch("chat_service.llm.registry.genai")
    def test_generate_with_fallback_gemini(self, mock_genai):
        """Test generate_with_fallback with GeminiLLM"""
        mock_genai.configure = Mock()
        mock_model = Mock()
        mock_model.generate_content.return_value.text = "SELECT * FROM test"
        mock_genai.GenerativeModel.return_value = mock_model
        
        llm = get_llm("sql")
        if llm.is_available():
            result = llm.generate("test")
            assert result is not None

