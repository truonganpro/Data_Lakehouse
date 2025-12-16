# -*- coding: utf-8 -*-
"""
Configuration for Chat Service
Centralized config for TRINO, LLM, SQL, QDRANT, etc.
"""
import os

# ====== Trino Configuration ======
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "chatbot")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "") or None
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
TRINO_DEFAULT_SCHEMA = os.getenv("TRINO_DEFAULT_SCHEMA", "gold")

# ====== LLM Configuration ======
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "none").lower()  # "gemini" | "openai" | "none"
LLM_MODEL_SQL = os.getenv("LLM_MODEL_SQL", "gpt-4o-mini")
LLM_MODEL_SUM = os.getenv("LLM_MODEL_SUM", "gpt-4o-mini")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")  # For OpenAI or Gemini

# ====== SQL Configuration ======
SQL_WHITELIST_SCHEMAS = set(os.getenv("SQL_WHITELIST_SCHEMAS", "gold,platinum,platinum_sys").split(","))
SQL_DEFAULT_LIMIT = int(os.getenv("SQL_DEFAULT_LIMIT", "200"))
SQL_MAX_ROWS = int(os.getenv("SQL_MAX_ROWS", "5000"))
SQL_TIMEOUT_SECS = int(os.getenv("SQL_TIMEOUT_SECS", "45"))

# ====== Qdrant Configuration ======
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

# ====== Logging Configuration ======
LOG_DB_URI = os.getenv("LOG_DB_URI", "")

# ====== Rate Limiting Configuration ======
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
RATE_LIMIT_WINDOW_S = int(os.getenv("RATE_LIMIT_WINDOW_S", "60"))
RATE_LIMIT_MAX_REQ = int(os.getenv("RATE_LIMIT_MAX_REQ", "30"))

# ====== Feature Flags ======
ENABLE_SUGGESTED_ACTIONS = os.getenv("ENABLE_SUGGESTED_ACTIONS", "true").lower() == "true"
ENABLE_EXPLANATION = os.getenv("ENABLE_EXPLANATION", "true").lower() == "true"

# ====== Security Configuration ======
MAX_REQUEST_SIZE = int(os.getenv("MAX_REQUEST_SIZE", "262144"))  # 256KB default
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# ====== Observability Configuration ======
ENABLE_METRICS = os.getenv("ENABLE_METRICS", "true").lower() == "true"
ENABLE_STRUCTURED_LOGS = os.getenv("ENABLE_STRUCTURED_LOGS", "true").lower() == "true"
ENABLE_TRACE_IDS = os.getenv("ENABLE_TRACE_IDS", "true").lower() == "true"

