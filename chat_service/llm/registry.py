# -*- coding: utf-8 -*-
"""
LLM Registry - Pluggable LLM providers with fallback support
"""
import os
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod

from core.config import (
    LLM_PROVIDER, LLM_MODEL_SQL, LLM_MODEL_SUM, LLM_API_KEY
)


class LLMBase(ABC):
    """Base class for LLM providers"""
    
    def __init__(self, model: str):
        self.model = model
    
    @abstractmethod
    def generate(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        """Generate text from prompt"""
        raise NotImplementedError
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if LLM provider is available"""
        raise NotImplementedError


class GeminiLLM(LLMBase):
    """Google Gemini LLM provider"""
    
    def __init__(self, model: str):
        super().__init__(model)
        self._client = None
        self._init_client()
    
    def _init_client(self):
        """Initialize Gemini client"""
        try:
            import google.generativeai as genai
            api_key = os.getenv("GOOGLE_API_KEY") or LLM_API_KEY
            if api_key:
                genai.configure(api_key=api_key)
                self._client = genai
            else:
                print("⚠️  GOOGLE_API_KEY not set, Gemini unavailable")
        except ImportError:
            print("⚠️  google-generativeai not installed, Gemini unavailable")
    
    def is_available(self) -> bool:
        """Check if Gemini is available"""
        return self._client is not None
    
    def generate(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        """Generate text using Gemini"""
        if not self.is_available():
            raise RuntimeError("Gemini is not available")
        
        try:
            from google.generativeai import GenerativeModel
            
            # Validate model name - Gemini models should start with "gemini-"
            # If model is OpenAI model (e.g., "gpt-4o-mini"), use default Gemini model
            if not self.model.startswith("gemini-"):
                print(f"⚠️  Invalid Gemini model '{self.model}', using default 'gemini-2.0-flash'")
                model_name = "gemini-2.0-flash"
            else:
                model_name = self.model
            
            model = GenerativeModel(model_name)
            
            # Combine system prompt and user prompt
            full_prompt = prompt
            if system:
                full_prompt = f"{system}\n\n{prompt}"
            
            response = model.generate_content(full_prompt)
            text = response.text.strip()
            
            # Clean up markdown code blocks if present
            text = text.strip("`").strip()
            if "```sql" in text:
                text = text.split("```sql")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            
            return text
        except Exception as e:
            print(f"❌ Gemini generation error: {e}")
            raise


class OpenAILLM(LLMBase):
    """OpenAI LLM provider"""
    
    def __init__(self, model: str):
        super().__init__(model)
        self._client = None
        self._init_client()
    
    def _init_client(self):
        """Initialize OpenAI client"""
        try:
            import openai
            api_key = os.getenv("OPENAI_API_KEY") or LLM_API_KEY
            if api_key:
                openai.api_key = api_key
                self._client = openai
            else:
                print("⚠️  OPENAI_API_KEY not set, OpenAI unavailable")
        except ImportError:
            print("⚠️  openai not installed, OpenAI unavailable")
    
    def is_available(self) -> bool:
        """Check if OpenAI is available"""
        return self._client is not None
    
    def generate(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        """Generate text using OpenAI"""
        if not self.is_available():
            raise RuntimeError("OpenAI is not available")
        
        try:
            import openai
            
            messages = []
            if system:
                messages.append({"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})
            
            response = self._client.ChatCompletion.create(
                model=self.model,
                messages=messages,
                **kwargs
            )
            
            text = response.choices[0].message.content.strip()
            return text
        except Exception as e:
            print(f"❌ OpenAI generation error: {e}")
            raise


class NoopLLM(LLMBase):
    """No-op LLM provider (fallback when no LLM is available)"""
    
    def __init__(self, model: str = "none"):
        super().__init__(model)
    
    def is_available(self) -> bool:
        """No-op is always available"""
        return True
    
    def generate(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        """Return empty string"""
        return ""


def get_llm(kind: str = "sql") -> LLMBase:
    """
    Get LLM instance based on configuration
    
    Args:
        kind: "sql" or "sum" (for SQL generation or summarization)
    
    Returns:
        LLM instance
    """
    model = LLM_MODEL_SQL if kind == "sql" else LLM_MODEL_SUM
    
    if LLM_PROVIDER == "gemini":
        return GeminiLLM(model)
    elif LLM_PROVIDER == "openai":
        return OpenAILLM(model)
    else:
        return NoopLLM(model)


def generate_with_fallback(
    prompt: str,
    kind: str = "sql",
    system: Optional[str] = None,
    **kwargs
) -> Optional[str]:
    """
    Generate text with automatic fallback to backup provider
    
    Args:
        prompt: User prompt
        kind: "sql" or "sum"
        system: Optional system prompt
        **kwargs: Additional arguments for LLM
    
    Returns:
        Generated text or None if all providers fail
    """
    primary = get_llm(kind)
    
    # Try primary provider
    if primary.is_available():
        try:
            result = primary.generate(prompt, system=system, **kwargs)
            if result:
                return result
        except Exception as e:
            print(f"⚠️  Primary LLM ({LLM_PROVIDER}) failed: {e}")
    
    # Fallback to backup provider
    backup_provider = "openai" if LLM_PROVIDER == "gemini" else "gemini"
    print(f"🔄 Falling back to {backup_provider}")
    
    # Temporarily switch provider
    original_provider = os.environ.get("LLM_PROVIDER", LLM_PROVIDER)
    os.environ["LLM_PROVIDER"] = backup_provider
    
    try:
        backup = get_llm(kind)
        if backup.is_available():
            result = backup.generate(prompt, system=system, **kwargs)
            if result:
                return result
    except Exception as e:
        print(f"⚠️  Backup LLM ({backup_provider}) failed: {e}")
    finally:
        # Restore original provider
        os.environ["LLM_PROVIDER"] = original_provider
    
    # All providers failed
    print("❌ All LLM providers failed")
    return None

