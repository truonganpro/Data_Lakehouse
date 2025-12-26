"""
Chat Service API
FastAPI service for SQL + RAG chatbot

Author: Truong An
Project: Data Lakehouse - Modern Data Stack
License: MIT
"""
import os
import re
import time
import uuid
from typing import Optional, List, Set, Tuple, Dict, Union
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import trino
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from qdrant_client import QdrantClient
from sqlalchemy import create_engine, text

from sql_templates import intent_to_sql, get_safe_schemas, get_example_questions, NO_SQL
from embeddings import embed_query
from llm_sql import gen_sql_with_gemini
from llm_summarize import format_answer, _parse_schema_from_sql, _explain_sql_and_lineage, dedupe_citations
from router import get_router
from errors import GuardError, GuardCode
from guard.auto_fix import ensure_limit, add_default_time_filter
from guard_message import message_and_suggestions
from suggestions import suggestions_for, suggestions_for_non_sql
from about_dataset_provider import get_about_dataset_card, top_tables_by_rows
from about_project_provider import get_about_project_card
from session_store import get_session_context, update_session_context
# Import from local modules (using relative imports since we're in /app)
from llm.registry import generate_with_fallback
from core.prompts import PROMPT_SQL, PROMPT_SUMMARY, PROMPT_EXPLAIN
from schema_summary import build_schema_summary
from guard.rate_limit import allow, get_remaining
from guard.quick_actions import suggest_actions
from metrics import record_request, record_rate_limit_block, get_metrics_prometheus
from chat_logging import log_chat

# SQL parsing for safety
try:
    import sqlglot
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    print("⚠️  sqlglot not available, falling back to regex-based safety")


# ============================================================================
# Configuration (import from core/config.py)
# ============================================================================

# Import core config
from core.config import (
    TRINO_HOST, TRINO_PORT, TRINO_CATALOG, TRINO_DEFAULT_SCHEMA,
    TRINO_USER, TRINO_PASSWORD,
    QDRANT_HOST, QDRANT_PORT,
    SQL_WHITELIST_SCHEMAS, SQL_DEFAULT_LIMIT, SQL_MAX_ROWS, SQL_TIMEOUT_SECS,
    LOG_DB_URI,
    RATE_LIMIT_ENABLED, RATE_LIMIT_WINDOW_S, RATE_LIMIT_MAX_REQ,
    ENABLE_SUGGESTED_ACTIONS, ENABLE_EXPLANATION,
    MAX_REQUEST_SIZE, CORS_ORIGINS,
    ENABLE_METRICS, ENABLE_STRUCTURED_LOGS, ENABLE_TRACE_IDS
)

# Initialize clients
qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

# Initialize logging engine if configured
log_engine = None
if LOG_DB_URI:
    try:
        log_engine = create_engine(LOG_DB_URI, pool_pre_ping=True)
    except Exception as e:
        print(f"⚠️  Could not initialize log database: {e}")


# ============================================================================
# FastAPI App
# ============================================================================

app = FastAPI(
    title="Lakehouse Chat Service",
    description="SQL + RAG Chatbot for Brazilian E-commerce Data Lakehouse",
    version="1.0.0"
)

# CORS (restrict to frontend domain)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)

# Trace ID Middleware
@app.middleware("http")
async def trace_id_middleware(request: Request, call_next):
    """Add trace ID to request and response"""
    if ENABLE_TRACE_IDS:
        trace_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]
        request.state.trace_id = trace_id
    else:
        request.state.trace_id = None
    response = await call_next(request)
    if ENABLE_TRACE_IDS and hasattr(request.state, "trace_id") and request.state.trace_id:
        response.headers["X-Request-ID"] = request.state.trace_id
    return response

# Request size limit middleware
@app.middleware("http")
async def request_size_limit(request: Request, call_next):
    """Limit request body size"""
    if request.method == "POST":
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > MAX_REQUEST_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"Request too large. Maximum size: {MAX_REQUEST_SIZE} bytes"
            )
    return await call_next(request)

# Rate Limiting Middleware
@app.middleware("http")
async def rate_limiter(request: Request, call_next):
    """Rate limiting middleware - block requests if over limit"""
    if not RATE_LIMIT_ENABLED:
        return await call_next(request)
    
    # Skip rate limiting for health checks and metrics
    if request.url.path in ["/health", "/healthz", "/docs", "/openapi.json", "/metrics"]:
        return await call_next(request)
    
    # Get client IP
    client_ip = request.client.host if request.client else "unknown"
    
    # Check rate limit
    if not allow(client_ip):
        remaining = get_remaining(client_ip)
        retry_after = RATE_LIMIT_WINDOW_S
        record_rate_limit_block()
        raise HTTPException(
            status_code=429,
            detail=f"Too Many Requests. Rate limit exceeded. Please try again later.",
            headers={
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Limit": str(RATE_LIMIT_MAX_REQ),
                "Retry-After": str(retry_after)
            }
        )
    
    # Add rate limit headers
    remaining = get_remaining(client_ip)
    response = await call_next(request)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Limit"] = str(RATE_LIMIT_MAX_REQ)
    return response


# ============================================================================
# Models
# ============================================================================

class AskRequest(BaseModel):
    session_id: Optional[str] = None
    question: str
    prefer_sql: Optional[bool] = True
    explain: Optional[bool] = False  # Toggle for SQL explanation


class QueryRequest(BaseModel):
    sql: str
    limit: Optional[int] = None
    explain: Optional[bool] = False  # Toggle for SQL explanation


class AskResponse(BaseModel):
    session_id: str
    answer: str
    sql: Optional[str] = None
    rows_preview: Optional[List[dict]] = None
    citations: Optional[List[dict]] = None
    execution_time_ms: Optional[int] = None
    suggestions: Optional[List[str]] = None  # Context-aware suggestions
    explanation: Optional[str] = None  # SQL explanation and lineage
    suggested_actions: Optional[List[dict]] = None  # Quick actions for guardrails


# ============================================================================
# SQL Safety & Execution
# ============================================================================

READONLY_PATTERN = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def _mask_sql_literals(sql: str) -> str:
    """
    Mask SQL literals for safe logging (hide sensitive data)
    
    Args:
        sql: SQL query string
        
    Returns:
        SQL with masked literals
    """
    # Replace string literals with ?
    masked = re.sub(r"'([^']*)'", "'?'", sql)
    # Replace numeric literals with ?
    masked = re.sub(r"\b\d+(\.\d+)?\b", "?", masked)
    return masked


def _parse_sql_schemas(sql: str) -> Set[str]:
    """
    Parse SQL to extract schemas using AST if available
    
    Args:
        sql: SQL query string
        
    Returns:
        Set of schema names found in the query
    """
    schemas = set()
    
    if not SQLGLOT_AVAILABLE:
        # Fallback: simple regex extraction
        sql_lower = sql.lower()
        for schema in SQL_WHITELIST_SCHEMAS:
            if f".{schema}." in sql_lower or f" {schema}." in sql_lower:
                schemas.add(schema)
        return schemas
    
    try:
        # Parse SQL with sqlglot
        parsed = sqlglot.parse_one(sql, dialect="trino")
        
        if not parsed:
            return set()
        
        # Extract schemas from tables
        for table in parsed.find_all(sqlglot.expressions.Table):
            if hasattr(table, "db") and table.db:
                schemas.add(table.db.lower())
        
        return schemas
        
    except Exception as e:
        print(f"⚠️  SQL parsing error: {e}, falling back to regex")
        # Fallback: regex extraction
        sql_lower = sql.lower()
        for schema in SQL_WHITELIST_SCHEMAS:
            if f".{schema}." in sql_lower or f" {schema}." in sql_lower:
                schemas.add(schema)
        return schemas


def _check_dangerous_keywords_with_ast(sql: str) -> Tuple[bool, Optional[str]]:
    """
    Check for dangerous keywords using AST (more accurate than substring)
    
    Args:
        sql: SQL query string
        
    Returns:
        Tuple of (has_dangerous_keyword, keyword_if_found)
    """
    dangerous = ["DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE", "SHOW", "EXPLAIN", "CALL", "EXEC", "EXECUTE"]
    
    if not SQLGLOT_AVAILABLE:
        # Fallback: substring check (original behavior)
        sql_upper = sql.upper()
        for keyword in dangerous:
            if keyword in sql_upper:
                return True, keyword
        return False, None
    
    try:
        parsed = sqlglot.parse_one(sql, dialect="trino")
        
        if not parsed:
            return False, None
        
        # Find dangerous operation types dynamically
        dangerous_types = []
        for attr in dir(sqlglot.expressions):
            if attr in ['Delete', 'Drop', 'Update', 'Insert', 'Create', 'AlterTable', 'Truncate', 'Show', 'Explain']:
                dangerous_types.append(getattr(sqlglot.expressions, attr))
        
        for dangerous_type in dangerous_types:
            if parsed.find(dangerous_type):
                return True, dangerous_type.__name__.replace("expressions.", "")
        
        return False, None
        
    except Exception as e:
        print(f"⚠️  AST keyword check error: {e}, falling back")
        # Fallback: substring check
        sql_upper = sql.upper()
        for keyword in dangerous:
            if keyword in sql_upper:
                return True, keyword
        return False, None


def _check_star_projection(sql: str) -> bool:
    """
    Check if SQL uses SELECT * (star projection)
    
    Args:
        sql: SQL query string
        
    Returns:
        True if SELECT * is found
    """
    if not SQLGLOT_AVAILABLE:
        # Fallback: regex check
        # Match SELECT * but not SELECT COUNT(*) or SELECT SUM(*)
        pattern = re.compile(r"SELECT\s+\*\s+FROM", re.IGNORECASE)
        return bool(pattern.search(sql))
    
    try:
        parsed = sqlglot.parse_one(sql, dialect="trino")
        if not parsed:
            return False
        
        # Find all SELECT statements
        for select in parsed.find_all(sqlglot.expressions.Select):
            for expr in select.expressions:
                if isinstance(expr, sqlglot.expressions.Star):
                    # Check if it's not inside an aggregate function
                    parent = expr.parent
                    if parent and isinstance(parent, (sqlglot.expressions.Count, sqlglot.expressions.Sum, sqlglot.expressions.Avg)):
                        continue
                    return True
        return False
    except Exception:
        # Fallback: regex
        pattern = re.compile(r"SELECT\s+\*\s+FROM", re.IGNORECASE)
        return bool(pattern.search(sql))


def _check_missing_time_predicate(sql: str) -> bool:
    """
    Check if SQL queries large fact tables without time predicate
    
    Args:
        sql: SQL query string
        
    Returns:
        True if missing time predicate for large fact tables
    """
    # Large fact tables that require time filter
    large_fact_tables = ["fact_order", "fact_order_item"]
    time_columns = ["full_date", "year_month", "order_date"]
    
    if not SQLGLOT_AVAILABLE:
        # Fallback: simple regex check
        sql_lower = sql.lower()
        has_large_fact = any(table in sql_lower for table in large_fact_tables)
        if not has_large_fact:
            return False
        
        # Check if any time column is used in WHERE clause
        has_time_pred = any(
            col in sql_lower and (
                f"where" in sql_lower and col in sql_lower.split("where")[1] if "where" in sql_lower else False
            )
            for col in time_columns
        )
        return not has_time_pred
    
    try:
        parsed = sqlglot.parse_one(sql, dialect="trino")
        if not parsed:
            return False
        
        # Check if query touches large fact tables
        has_large_fact = False
        for table in parsed.find_all(sqlglot.expressions.Table):
            table_name = table.name.lower() if hasattr(table, "name") else ""
            if any(ft in table_name for ft in large_fact_tables):
                has_large_fact = True
                break
        
        if not has_large_fact:
            return False
        
        # Check if WHERE clause contains time predicates
        where_clauses = parsed.find_all(sqlglot.expressions.Where)
        if not where_clauses:
            return True  # No WHERE clause = missing time predicate
        
        # Check if any time column is referenced in WHERE
        where_str = str(where_clauses[0]).lower()
        has_time_pred = any(col in where_str for col in time_columns)
        
        return not has_time_pred
        
    except Exception:
        # Fallback: simple check
        sql_lower = sql.lower()
        has_large_fact = any(table in sql_lower for table in large_fact_tables)
        if not has_large_fact:
            return False
        
        has_time_pred = any(col in sql_lower for col in time_columns)
        return not has_time_pred


def enforce_sql_safety(sql: str, raise_guard_error: bool = True) -> str:
    """
    Enforce SQL safety constraints using AST when available
    
    Args:
        sql: SQL query string
        raise_guard_error: If True, raise GuardError instead of HTTPException
    
    Raises:
        GuardError if raise_guard_error=True and SQL is unsafe
        HTTPException if raise_guard_error=False and SQL is unsafe
        
    Returns:
        Modified SQL with safety constraints
    """
    if not sql or not sql.strip():
        if raise_guard_error:
            raise GuardError(GuardCode.AMBIGUOUS_INTENT, "SQL query is empty")
        raise HTTPException(status_code=400, detail="SQL query is empty")
    
    # Must be SELECT or WITH (read-only)
    if not READONLY_PATTERN.match(sql):
        if raise_guard_error:
            raise GuardError(GuardCode.BANNED_FUNC, "Only SELECT and WITH queries are allowed (read-only)")
        raise HTTPException(
            status_code=400,
            detail="Only SELECT and WITH queries are allowed (read-only)"
        )
    
    # Check for dangerous keywords using AST (more accurate)
    has_dangerous, keyword = _check_dangerous_keywords_with_ast(sql)
    if has_dangerous:
        if raise_guard_error:
            raise GuardError(GuardCode.BANNED_FUNC, f"Dangerous keyword '{keyword}' not allowed")
        raise HTTPException(
            status_code=400,
            detail=f"Dangerous keyword '{keyword}' not allowed"
        )
    
    # Check for SELECT * (star projection)
    if _check_star_projection(sql):
        if raise_guard_error:
            raise GuardError(GuardCode.STAR_PROJECTION, "SELECT * is not allowed for safety")
        raise HTTPException(
            status_code=400,
            detail="SELECT * is not allowed for safety"
        )
    
    # Check whitelist schemas using AST (more accurate)
    schemas = _parse_sql_schemas(sql)
    has_safe_schema = bool(schemas.intersection(SQL_WHITELIST_SCHEMAS))
    
    if not has_safe_schema:
        if raise_guard_error:
            raise GuardError(GuardCode.DISALLOWED_SCHEMA, f"Query must use one of these schemas: {', '.join(SQL_WHITELIST_SCHEMAS)}")
        raise HTTPException(
            status_code=400,
            detail=f"Query must use one of these schemas: {', '.join(SQL_WHITELIST_SCHEMAS)}"
        )
    
    # Auto-fix: Add time filter for large fact tables if missing
    # Check before applying auto-fix
    issues_to_fix = []
    if _check_missing_time_predicate(sql):
        issues_to_fix.append(GuardCode.MISSING_TIME_PRED)
    
    # Auto-fix: Add LIMIT if missing
    sql_upper = sql.upper()
    has_limit = bool(re.search(r"\bLIMIT\s+\d+\b", sql_upper))
    if not has_limit:
        issues_to_fix.append(GuardCode.MISSING_LIMIT)
    
    # Apply auto-fixes (instead of raising errors)
    if issues_to_fix:
        if GuardCode.MISSING_TIME_PRED in issues_to_fix:
            sql = add_default_time_filter(sql, months=3)
            print(f"✅ Auto-fixed: Added default time filter (last 3 months)")
        
        if GuardCode.MISSING_LIMIT in issues_to_fix:
            sql = ensure_limit(sql, default_limit=SQL_DEFAULT_LIMIT)
            print(f"✅ Auto-fixed: Added LIMIT {SQL_DEFAULT_LIMIT}")
    
    return sql


def enrich_with_product_info(rows: list[dict]) -> list[dict]:
    """
    If result has product_id, attach product info from dim tables.
    This is a safety net: even if SQL doesn't join dim tables, we enrich here.
    """
    if not rows or "product_id" not in rows[0]:
        return rows
    
    # Get unique product_ids, limit for safety
    pids = list({r["product_id"] for r in rows if r.get("product_id")})
    if not pids:
        return rows
    pids = pids[:500]  # safety cap
    
    # Build lookup SQL
    placeholders = ",".join(f"'{pid}'" for pid in pids)
    lookup_sql = f"""
    SELECT
      p.product_id,
      COALESCE(pc.product_category_name_english, 'unknown') AS category_en,
      p.product_weight_g,
      p.product_length_cm,
      p.product_height_cm,
      p.product_width_cm
    FROM {TRINO_CATALOG}.gold.dim_product p
    LEFT JOIN {TRINO_CATALOG}.gold.dim_product_category pc
      ON pc.product_category_name = p.product_category_name
    WHERE p.product_id IN ({placeholders})
    LIMIT {len(pids)}
    """
    
    try:
        # Run lookup through run_sql (reuses guardrails)
        info_rows, _ = run_sql(lookup_sql, schema="gold")
        info_map = {r["product_id"]: r for r in info_rows}
        
        # Merge into original results (don't overwrite existing columns)
        enriched = []
        for r in rows:
            pid = r.get("product_id")
            if pid in info_map:
                # Merge: existing values take priority
                merged = {**info_map[pid], **r}
                enriched.append(merged)
            else:
                enriched.append(r)
        
        return enriched
    except Exception as e:
        # If enrichment fails, return original rows
        print(f"⚠️  Product enrichment failed: {e}")
        return rows


def run_sql(sql: str, schema: str = TRINO_DEFAULT_SCHEMA, check_empty: bool = False) -> tuple:
    """
    Execute SQL query on Trino
    
    Args:
        sql: SQL query string
        schema: Default schema
        check_empty: If True, raise GuardError when no rows returned (default: False)
        
    Returns:
        Tuple of (rows, execution_time_ms)
        
    Raises:
        GuardError if check_empty=True and no rows returned
        HTTPException for other SQL errors
    """
    # Use GuardError for better error handling
    sql = enforce_sql_safety(sql, raise_guard_error=True)
    
    start_time = time.time()
    
    try:
        with connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=schema,
            http_scheme="http",
            auth=None if not TRINO_PASSWORD else BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
            source="chat-service"
        ) as conn:
            cur = conn.cursor()
            
            # Set query timeout
            cur.execute(f"SET SESSION query_max_run_time = '{SQL_TIMEOUT_SECS}s'")
            
            # Execute query
            cur.execute(sql)
            
            # Fetch results
            rows = cur.fetchmany(SQL_MAX_ROWS + 1)
            columns = [desc[0] for desc in cur.description]
            
            # Check if we hit the limit
            if len(rows) > SQL_MAX_ROWS:
                rows = rows[:SQL_MAX_ROWS]
            
            # Convert to list of dicts
            result = [dict(zip(columns, row)) for row in rows]
            
            # Check for empty result if requested
            if check_empty and len(result) == 0:
                raise GuardError(GuardCode.NO_DATA, "Query returned 0 rows")
            
    except GuardError:
        # Re-raise GuardError as-is
        raise
    except Exception as e:
        # Wrap other errors as GuardError or HTTPException
        error_msg = str(e)
        if "TABLE_NOT_FOUND" in error_msg or "does not exist" in error_msg:
            raise GuardError(GuardCode.DISALLOWED_SCHEMA, f"Table not found: {error_msg}")
        elif "SYNTAX_ERROR" in error_msg or "syntax" in error_msg.lower():
            raise GuardError(GuardCode.AMBIGUOUS_INTENT, f"SQL syntax error: {error_msg}")
        else:
            # For other errors, raise HTTPException (backward compatibility)
            raise HTTPException(
                status_code=500,
                detail=f"SQL execution error: {error_msg}"
            )
    
    execution_time = int((time.time() - start_time) * 1000)
    
    return result, execution_time


# ============================================================================
# SQL Generation (Template + Gemini)
# ============================================================================

def build_sql(question: str) -> Tuple[Optional[str], Optional[Dict]]:
    """
    Build SQL query using multi-tier approach:
    1. Check non-SQL modes (smalltalk, about_data, about_project) - quick check
    2. Try router + skills FIRST (optimized queries, e.g., platinum datamart)
    3. Fallback to legacy templates (backward compatibility)
    4. Fallback to Gemini LLM (for complex queries)
    
    Args:
        question: User's question
        
    Returns:
        Tuple of (SQL query string or NO_SQL or empty string or None, metadata dict or None)
        - SQL string: Normal SQL query
        - Empty string "": HELP mode
        - NO_SQL: Non-SQL response (smalltalk, about_data, about_project)
        - None: No match found
        - metadata: Dict with topic info for non-SQL responses
    """
    q_lower = question.lower().strip()
    
    # 0. PRIORITY: Check for forecast metric/concept questions FIRST (before smalltalk)
    # These are questions about definitions, not data queries - should use RAG + LLM
    # Pattern 1: Exact matches for "X là gì" or "X nghĩa là"
    has_forecast_metric_question = any(
        kw in q_lower
        for kw in [
            "smape là gì",
            "mae là gì",
            "rmse là gì",
            "mape là gì",
            "ci coverage là gì",
            "yhat_lo là gì",
            "yhat_hi là gì",
            "yhat là gì",
            "forecast metric",
            "forecast accuracy",
            "độ chính xác dự báo",
            "smape bao nhiêu",
            "mae bao nhiêu",
            "rmse bao nhiêu",
            "smape tốt",
            "mae tốt",
            "rmse tốt",
            "smape nghĩa là",
            "mae nghĩa là",
            "rmse nghĩa là",
            "đánh giá chất lượng forecast",
            "đánh giá chất lượng dự báo",
            "forecast monitoring",
            "backtest đo",
            "ý nghĩa của",  # "Ý nghĩa của CI width?"
        ]
    ) or (
        # Pattern 2: Metric keyword + question words (conceptual question)
        any(kw in q_lower for kw in ["smape", "mae", "rmse", "mape", "ci coverage", "yhat_lo", "yhat_hi", "forecast accuracy", "forecast metric", "ci width", "ci interval"])
        and any(kw in q_lower for kw in ["là gì", "nghĩa là", "định nghĩa", "bao nhiêu", "tốt", "what is", "meaning", "đánh giá", "chất lượng", "ý nghĩa"])
        # BUT NOT data query keywords (to distinguish from ForecastMetricsSkill)
        and not any(kw in q_lower for kw in ["so sánh", "compare", "của các model", "tháng", "month", "năm", "year", "trung bình", "average", "tổng", "sum"])
    )
    
    # If it's a forecast metric question (conceptual, not data query), use RAG + LLM
    if has_forecast_metric_question:
        print(f"✅ Detected forecast metric question: {question}")
        return NO_SQL, {"topic": "about_forecast_metric"}
    
    # 1. Quick check for non-SQL modes (smalltalk, about_data, about_project)
    # Use direct check instead of intent_to_sql to avoid legacy template matching
    from sql_templates import SMALLTALK_TRIGGERS, ABOUT_DATA_TRIGGERS, ABOUT_PROJECT_TRIGGERS, HELP_TRIGGERS
    
    if any(trigger in q_lower for trigger in SMALLTALK_TRIGGERS):
        # Check if it's a personal question
        is_personal = any(kw in q_lower for kw in ["bạn là ai", "tôi là ai", "bạn biết tôi", "who are you", "who am i"])
        if is_personal or not _has_data_entities_in_question(question):
            return NO_SQL, {"topic": "smalltalk"}
    
    # Chặn intent "about dataset" không bắt các câu có từ khóa forecast
    # Mọi câu có từ khóa forecast thì luôn ưu tiên đi qua router/skills
    has_forecast_kw = any(kw in q_lower for kw in ["dự báo", "forecast", "horizon", "ci ", "kịch bản", 
                                                    "smape", "mae", "rmse", "monitoring", "backtest",
                                                    "yhat", "yhat_lo", "yhat_hi", "planning"])
    
    # Check ABOUT_DATA_STATS triggers first (table size queries)
    from sql_templates import ABOUT_DATA_STATS_TRIGGERS
    if any(trigger in q_lower for trigger in ABOUT_DATA_STATS_TRIGGERS):
        return NO_SQL, {"topic": "about_data_stats"}
    
    # Chỉ check ABOUT_DATA_TRIGGERS nếu KHÔNG có từ khóa forecast
    if not has_forecast_kw and any(trigger in q_lower for trigger in ABOUT_DATA_TRIGGERS):
        return NO_SQL, {"topic": "about_data"}
    
    if any(trigger in q_lower for trigger in ABOUT_PROJECT_TRIGGERS):
        return NO_SQL, {"topic": "about_project"}
    
    if any(trigger in q_lower for trigger in HELP_TRIGGERS):
        return "", None
    
    # 2. Try router + skills system FIRST (priority - optimized queries)
    # This ensures we use platinum datamart for monthly revenue queries
    try:
        router = get_router()
        sql, router_metadata = router.generate_sql(question, threshold=0.6)
        
        if sql:
            skill_name = router_metadata.get('skill_name', 'unknown')
            confidence = router_metadata.get('confidence', 0)
            print(f"✅ Using skill '{skill_name}' (confidence: {confidence:.2f})")
            return sql, router_metadata
    except Exception as e:
        print(f"⚠️  Router error: {e}")
    
    # 3. Fallback to legacy templates (for backward compatibility)
    # Only use if router didn't match
    sql, metadata = intent_to_sql(question)
    
    if sql == NO_SQL:
        topic = metadata.get("topic") if metadata else "unknown"
        print(f"💬 Non-SQL mode triggered: {topic} for: {question}")
        return NO_SQL, metadata
    
    if sql == "":
        print(f"💡 HELP MODE triggered for: {question}")
        return "", None
    
    if sql:
        print(f"✅ Using legacy template SQL for: {question}")
        return sql, None
    
    # 4. Fallback to LLM (pluggable provider: Gemini/OpenAI)
    print(f"🤖 Falling back to LLM for: {question}")
    try:
        # Build prompt with schema summary injected
        schema_summary = build_schema_summary()
        full_prompt = PROMPT_SQL.format(
            schema_summary=schema_summary,
            question=question
        )
        
        sql = generate_with_fallback(
            prompt=full_prompt,
            kind="sql",
            system=None  # Full prompt already includes everything
        )
        if sql:
            return sql, None
    except Exception as e:
        print(f"⚠️  LLM generation error: {e}")
        # Fallback to legacy Gemini if available
        if os.getenv("LLM_PROVIDER", "none").lower() == "gemini":
            sql = gen_sql_with_gemini(question)
            if sql:
                return sql, None
    
    # 5. No SQL generated
    return None, None


def _has_data_entities_in_question(question: str) -> bool:
    """Quick check if question contains data-related keywords"""
    q_lower = question.lower()
    data_keywords = [
        "doanh thu", "revenue", "gmv", "orders", "đơn hàng", "sản phẩm", "product",
        "tháng", "month", "tuần", "week", "ngày", "day", "năm", "year",
        "thanh toán", "payment", "khách hàng", "customer", "seller", "người bán",
        "danh mục", "category", "vùng", "region", "bang", "state"
    ]
    return any(kw in q_lower for kw in data_keywords)


# ============================================================================
# RAG (Retrieval-Augmented Generation)
# ============================================================================

def rag_search(question: str, k: int = 4) -> List[dict]:
    """
    Search for relevant documents using RAG with deduplication by source
    
    Args:
        question: User's question
        k: Number of results to return
        
    Returns:
        List of relevant document chunks (deduplicated by source, keeping highest score)
    """
    try:
        vector = embed_query(question)
        
        # Lấy dư để còn dedupe (lấy k * 5 hoặc tối thiểu 20)
        hits = qdrant_client.search(
            collection_name="knowledge_base",
            query_vector=vector,
            limit=max(k * 5, 20)
        )
        
        # Dedupe theo source, giữ hit có score cao nhất cho mỗi source
        best_by_source = {}
        for hit in hits:
            source = (hit.payload or {}).get("source", "unknown")
            
            item = {
                "doc_id": hit.id,
                "score": float(hit.score or 0),
                "text": ((hit.payload or {}).get("text", "") or "")[:500],
                "source": source,
            }
            
            # Giữ chunk có score cao nhất cho mỗi source
            if source not in best_by_source or item["score"] > best_by_source[source]["score"]:
                best_by_source[source] = item
        
        # Sắp xếp theo score giảm dần và lấy k đầu tiên
        unique = sorted(best_by_source.values(), key=lambda x: x["score"], reverse=True)
        return unique[:k]
        
    except Exception as e:
        print(f"⚠️  RAG search error: {e}")
        return []


# ============================================================================
# Logging
# ============================================================================

def log_conversation(session_id: str, role: str, content: str):
    """Log conversation message"""
    if not log_engine:
        return
    
    try:
        with log_engine.connect() as conn:
            conn.execute(
                text("INSERT INTO messages (session_id, role, content) VALUES (:sid, :role, :content)"),
                {"sid": session_id, "role": role, "content": content}
            )
            conn.commit()
    except Exception as e:
        print(f"⚠️  Logging error: {e}")


def log_sql_execution(session_id: str, sql: str, rowcount: int, duration_ms: int, error: str = None, trace_id: str = None):
    """Log SQL execution with masked literals for privacy"""
    if not log_engine:
        return
    
    try:
        # Mask sensitive literals before logging
        safe_sql = _mask_sql_literals(sql)
        
        with log_engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO sql_audit 
                    (session_id, generated_sql, executed_sql, is_readonly, rowcount, duration_ms, error, created_at)
                    VALUES (:sid, :sql, :sql, 1, :rows, :dur, :err, NOW())
                """),
                {
                    "sid": session_id,
                    "sql": safe_sql,
                    "rows": rowcount,
                    "dur": duration_ms,
                    "err": error
                }
            )
            conn.commit()
    except Exception as e:
        print(f"⚠️  SQL logging error: {e}")
    
    # Also log to console if structured logging is enabled
    if ENABLE_STRUCTURED_LOGS:
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "trace_id": trace_id,
            "rowcount": rowcount,
            "duration_ms": duration_ms,
            "error": error is not None,
            "sql_hash": hash(safe_sql) if 'safe_sql' in locals() else None
        }
        print(f"📊 SQL Execution: {log_entry}")


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
@app.get("/healthz")  # Thêm alias /healthz
def health_check():
    """Health check endpoint - kiểm tra kết nối Trino và Qdrant"""
    # Simple health check - verify connections
    trino_ok = False
    qdrant_ok = False
    
    try:
        # Quick Trino connection test
        with connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            http_scheme="http",
            auth=None if not TRINO_PASSWORD else BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
            source="health-check"
        ) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            trino_ok = True
    except Exception as e:
        print(f"⚠️  Trino health check failed: {e}")
    
    try:
        # Quick Qdrant connection test
        collections = qdrant_client.get_collections()
        qdrant_ok = True
    except Exception as e:
        print(f"⚠️  Qdrant health check failed: {e}")
    
    status = "healthy" if (trino_ok and qdrant_ok) else "degraded"
    
    return {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "trino": {
                "host": f"{TRINO_HOST}:{TRINO_PORT}",
                "status": "ok" if trino_ok else "error"
            },
            "qdrant": {
                "host": f"{QDRANT_HOST}:{QDRANT_PORT}",
                "status": "ok" if qdrant_ok else "error"
            }
        }
    }


@app.get("/examples")
def get_examples():
    """Get example questions"""
    return {
        "examples": get_example_questions()
    }


@app.get("/metrics")
def metrics_endpoint():
    """Prometheus metrics endpoint"""
    if not ENABLE_METRICS:
        from fastapi.responses import JSONResponse
        return JSONResponse({"message": "Metrics disabled"})
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(get_metrics_prometheus(), media_type="text/plain")


@app.post("/query")
def run_query(request: QueryRequest, http_request: Request = None):
    """
    Run SQL query directly (with optional explanation)
    
    Args:
        request: QueryRequest with SQL and optional explain flag
        http_request: HTTP request object (for trace ID)
    
    Returns:
        Query results with optional explanation
    """
    sql = request.sql.strip()
    limit = request.limit or SQL_DEFAULT_LIMIT
    explain = request.explain or False
    
    if not sql:
        raise HTTPException(status_code=400, detail="SQL cannot be empty")
    
    # Get trace ID if available
    trace_id = getattr(http_request.state, "trace_id", None) if http_request and hasattr(http_request, "state") else None
    
    # Execute SQL
    start_time = time.time()
    try:
        # run_sql returns (result: List[Dict], execution_time_ms: int)
        result, exec_time = run_sql(sql)
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # result is already a list of dicts, extract columns from first row if available
        rows_preview = result
        cols = list(rows_preview[0].keys()) if rows_preview and len(rows_preview) > 0 else []
        
        # Generate explanation if requested and enabled
        explanation = None
        if ENABLE_EXPLANATION and explain:
            source_schema = _parse_schema_from_sql(sql)
            explanation = _explain_sql_and_lineage(sql, source_schema, rows_preview)
        
        # Log execution
        log_sql_execution("query", sql, len(rows_preview), execution_time_ms, error=None, trace_id=trace_id)
        
        # Record metrics
        record_request("/query", 200, execution_time_ms, len(rows_preview), error=False)
        
        return {
            "columns": cols,
            "rows": rows_preview,
            "execution_time_ms": execution_time_ms,
            "explanation": explanation if ENABLE_EXPLANATION else None
        }
    except HTTPException as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        log_sql_execution("query", sql, 0, execution_time_ms, error=str(e.detail), trace_id=trace_id)
        record_request("/query", e.status_code, execution_time_ms, 0, error=True)
        raise
    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        log_sql_execution("query", sql, 0, execution_time_ms, error=str(e), trace_id=trace_id)
        record_request("/query", 500, execution_time_ms, 0, error=True)
        raise HTTPException(status_code=500, detail=f"SQL execution error: {str(e)}")


@app.post("/explain")
def explain_sql_endpoint(request: Dict, http_request: Request = None):
    """
    Explain SQL query (lineage, measures, dimensions)
    
    Args:
        request: Dict with 'sql' key
        http_request: HTTP request object (for trace ID)
    
    Returns:
        Explanation text
    """
    sql = request.get("sql", "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL cannot be empty")
    
    # Get trace ID if available
    trace_id = getattr(http_request.state, "trace_id", None) if http_request and hasattr(http_request, "state") else None
    
    start_time = time.time()
    try:
        source_schema = _parse_schema_from_sql(sql)
        explanation = _explain_sql_and_lineage(sql, source_schema, None)
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Record metrics
        record_request("/explain", 200, execution_time_ms, 0, error=False)
        
        return {
            "sql": sql,
            "explanation": explanation
        }
    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        record_request("/explain", 500, execution_time_ms, 0, error=True)
        raise HTTPException(status_code=500, detail=f"Explanation error: {str(e)}")


@app.post("/ask", response_model=AskResponse)
def ask(request: AskRequest, http_request: Request = None):
    """
    Main chat endpoint with Gemini integration + HELP MODE
    
    Flow:
    1. Check for HELP intent (general questions)
    2. Generate SQL (template → Gemini fallback)
    3. Execute SQL on Trino (with guardrails)
    4. Search RAG citations
    5. Summarize with Gemini (if enabled)
    """
    # Generate session ID if not provided
    session_id = request.session_id or uuid.uuid4().hex[:16]
    question = request.question.strip()
    
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    
    # Get trace ID if available
    trace_id = getattr(http_request.state, "trace_id", None) if http_request and hasattr(http_request, "state") else None
    
    # Start timing
    start_time = time.time()
    
    # Log user question
    log_conversation(session_id, "user", question)
    
    sql_query = None
    rows_preview = None
    citations = None
    total_execution_time = 0
    error_msg = None
    non_sql_metadata = None
    skill_metadata = None
    guard_code = None
    source_schema = None
    suggestions = None
    
    # 0. Get session context (if available) for context-aware responses
    session_context = get_session_context(session_id)
    
    # 1. Generate SQL (or detect non-SQL modes)
    # Always call build_sql to detect non-SQL modes, regardless of prefer_sql
    sql_query, metadata = build_sql(question)
    
    # Store metadata for suggestions
    if metadata:
        if sql_query and sql_query != NO_SQL and sql_query != "":
            # SQL was generated, metadata is from router/skill
            skill_metadata = metadata
        else:
            # Non-SQL mode, metadata is from intent_to_sql
            non_sql_metadata = metadata
    elif sql_query and sql_query != NO_SQL and sql_query != "":
        # SQL generated but no metadata (legacy template)
        skill_metadata = {}
    
    # 1a. Non-SQL modes (smalltalk, about_data, about_project, about_forecast_metric)
    # Handle these BEFORE checking prefer_sql, as they don't need SQL
    if sql_query == NO_SQL:
        topic = metadata.get("topic") if metadata else "unknown"
        non_sql_metadata = metadata
        
        print(f"💬 Non-SQL mode detected: topic={topic} for question: {question}")
        
        # PRIORITY: about_forecast_metric (conceptual questions about forecast metrics)
        if topic == "about_forecast_metric":
            # Use RAG + LLM to answer conceptual questions about forecast metrics
            print(f"🔍 Searching RAG for forecast metric question: {question}")
            citations = rag_search(question, k=4)
            print(f"✅ Found {len(citations) if citations else 0} citations from RAG")
            
            # Generate answer using LLM with RAG context
            from llm_summarize import summarize_docs_with_llm
            answer = summarize_docs_with_llm(question, citations)
            
            # Fallback if LLM fails: try to extract answer directly from citations
            if not answer:
                print(f"⚠️  LLM failed to generate answer, trying to extract from citations")
                
                # Try to find relevant information in citations
                q_lower = question.lower()
                answer_parts = []
                
                # Check for sMAPE questions
                if "smape" in q_lower:
                    for cite in citations:
                        text = cite.get('text', '').lower()
                        if 'smape' in text or 'symmetric mean absolute percentage error' in text:
                            # Extract relevant parts
                            if '20%' in text or '30%' in text or '40%' in text:
                                answer_parts.append(
                                    "**sMAPE (Symmetric Mean Absolute Percentage Error)** là một metric đo độ chính xác của forecast.\n\n"
                                    "**Ngưỡng đánh giá:**\n"
                                    "  • < 20%: Rất tốt\n"
                                    "  • 20-30%: Tốt\n"
                                    "  • 30-40%: Trung bình\n"
                                    "  • > 40%: Cần cải thiện\n\n"
                                    f"(Nguồn: {cite.get('source', 'forecast documentation')})"
                                )
                                break
                
                # Check for MAE questions
                elif "mae" in q_lower and "mean absolute error" not in q_lower:
                    answer_parts.append(
                        "**MAE (Mean Absolute Error)** là metric đo độ lệch trung bình giữa giá trị dự báo và giá trị thực tế.\n\n"
                        "MAE càng nhỏ thì forecast càng chính xác.\n\n"
                        "(Thông tin chi tiết có trong tài liệu forecast)"
                    )
                
                # Check for RMSE questions
                elif "rmse" in q_lower:
                    answer_parts.append(
                        "**RMSE (Root Mean Squared Error)** là metric đo độ lệch bình phương trung bình giữa giá trị dự báo và giá trị thực tế.\n\n"
                        "RMSE càng nhỏ thì forecast càng chính xác. RMSE thường lớn hơn MAE vì nó phạt nặng hơn các lỗi lớn.\n\n"
                        "(Thông tin chi tiết có trong tài liệu forecast)"
                    )
                
                # Check for CI coverage questions
                elif "ci coverage" in q_lower or "ci width" in q_lower:
                    answer_parts.append(
                        "**CI Coverage (Confidence Interval Coverage)** là tỷ lệ giá trị thực tế nằm trong khoảng confidence interval (yhat_lo, yhat_hi) của forecast.\n\n"
                        "CI Coverage càng gần 95% (nếu dùng 95% CI) thì forecast càng đáng tin cậy.\n\n"
                        "(Thông tin chi tiết có trong tài liệu forecast)"
                    )
                
                # Generic fallback
                if not answer_parts:
                    answer = (
                        f"Tôi không thể tìm thấy thông tin chi tiết về '{question}' trong tài liệu hiện tại.\n\n"
                        "💡 **Gợi ý:**\n"
                        "  • Thử hỏi: \"sMAPE là gì?\" hoặc \"MAE là gì?\"\n"
                        "  • Hoặc hỏi về dữ liệu: \"So sánh sMAPE của các model forecast?\"\n"
                    )
                else:
                    answer = "\n\n".join(answer_parts)
                    print(f"✅ Extracted answer from citations ({len(answer)} chars)")
            else:
                print(f"✅ Generated answer from RAG + LLM ({len(answer)} chars)")
            
            # Build citations for display (dedupe để chắc chắn 100%)
            citations_unique = dedupe_citations(citations, max_items=4) if citations else []
            citations_display = [
                {
                    "doc_id": cite.get("doc_id", ""),
                    "score": cite.get("score", 0.0),
                    "text": cite.get("text", "")[:200],
                    "source": cite.get("source", "unknown"),
                }
                for cite in citations_unique
            ] if citations_unique else None
            
            log_conversation(session_id, "assistant", answer)
            
            return AskResponse(
                session_id=session_id,
                answer=answer,
                sql=None,
                rows_preview=None,
                citations=citations_display,
                execution_time_ms=int((time.time() - start_time) * 1000),
                suggestions=suggestions_for_non_sql(topic)
            )
        
        if topic == "smalltalk":
                # Check if it's a personal question
                q_lower = question.lower()
                is_personal = any(kw in q_lower for kw in ["bạn là ai", "tôi là ai", "bạn biết tôi", "who are you", "who am i", "what is your name", "tên bạn"])
                
                if is_personal:
                    answer = (
                        "Xin chào! 👋 Mình là **trợ lý phân tích dữ liệu** cho hệ thống Brazilian E-commerce (Olist).\n\n"
                        "**🎯 Chức năng của mình:**\n"
                        "  • 🔍 **Truy vấn SQL thông minh**: Chuyển đổi câu hỏi tự nhiên thành SQL queries\n"
                        "  • 📊 **Phân tích dữ liệu**: Doanh thu, sản phẩm, đơn hàng, thanh toán, logistics\n"
                        "  • 📈 **KPI & Metrics**: GMV, AOV, retention, on-time rate, payment mix\n"
                        "  • 🤖 **Tóm tắt kết quả**: Tự động tóm tắt insights từ dữ liệu (nếu có Gemini API)\n"
                        "  • 📚 **RAG Citations**: Trích dẫn tài liệu liên quan khi giải thích KPI\n\n"
                        "**🔒 Bảo mật & Quyền riêng tư:**\n"
                        "  • Mình **không lưu** thông tin cá nhân của bạn\n"
                        "  • Mình **không nhận diện** người dùng (mọi session độc lập)\n"
                        "  • Chỉ truy vấn **read-only** trên schema `gold` và `platinum`\n"
                        "  • Tự động áp dụng **LIMIT** và **timeout** để bảo vệ hệ thống\n\n"
                        "**💡 Ví dụ câu hỏi bạn có thể hỏi:**\n"
                        "  • \"Doanh thu theo tháng gần đây?\"\n"
                        "  • \"Top 10 sản phẩm bán chạy nhất năm 2017?\"\n"
                        "  • \"Phân bố đơn hàng theo bang?\"\n"
                        "  • \"Tỷ lệ giao hàng đúng hạn theo tháng?\"\n"
                        "  • \"Cohort retention của khách hàng?\"\n\n"
                        "Hãy thử hỏi mình bất kỳ câu hỏi nào về dữ liệu Olist! 🚀"
                    )
                else:
                    # General greeting or thanks
                    if any(kw in q_lower for kw in ["cảm ơn", "thanks", "thank you", "thank"]):
                        answer = (
                            "Không có gì! 😊\n\n"
                            "Mình rất vui được giúp bạn phân tích dữ liệu. "
                            "Nếu bạn có thêm câu hỏi nào khác về Olist data, cứ hỏi mình nhé!\n\n"
                            "💡 **Gợi ý tiếp theo:**\n"
                            "  • So sánh doanh thu theo quý\n"
                            "  • Phân tích xu hướng thanh toán\n"
                            "  • Top seller có on-time rate cao nhất"
                        )
                    elif any(kw in q_lower for kw in ["tạm biệt", "bye", "goodbye", "see you"]):
                        answer = (
                            "Tạm biệt bạn! 👋\n\n"
                            "Cảm ơn bạn đã sử dụng dịch vụ. Chúc bạn một ngày tốt lành!\n\n"
                            "Nếu cần phân tích dữ liệu, cứ quay lại hỏi mình nhé! 😊"
                        )
                    else:
                        answer = (
                            "Chào bạn! 👋\n\n"
                            "Mình là trợ lý phân tích dữ liệu Olist. Mình có thể giúp bạn:\n\n"
                            "**📊 Phân tích số liệu:**\n"
                            "  • Doanh thu, GMV, AOV theo thời gian/danh mục/vùng\n"
                            "  • Top sản phẩm, seller, danh mục\n"
                            "  • Phân tích cohort & retention khách hàng\n"
                            "  • SLA logistics (on-time rate, delivery days)\n"
                            "  • Payment mix và xu hướng thanh toán\n\n"
                            "**💡 Ví dụ câu hỏi:**\n"
                            "  • \"Doanh thu 3 tháng gần đây?\"\n"
                            "  • \"Top 10 sản phẩm bán chạy?\"\n"
                            "  • \"Phân bố đơn hàng theo bang?\"\n\n"
                            "Hãy hỏi mình bất kỳ câu hỏi nào về dữ liệu! 🚀"
                        )
        elif topic == "about_data_stats":
            # Handle table size/row count queries from metadata
            from about_dataset_provider import top_tables_by_rows
            try:
                rows = top_tables_by_rows(5)
                if not rows:
                    # No metadata available
                    answer = (
                        "Chưa có metadata `platinum_sys.data_catalog` để thống kê bảng theo số dòng.\n\n"
                        "💡 **Gợi ý:**\n"
                        "  • Cập nhật metadata lakehouse\n"
                        "  • Liệt kê bảng platinum\n"
                        "  • Dataset của mình là gì?"
                    )
                    suggestions = [
                        "cập nhật metadata lakehouse",
                        "liệt kê bảng platinum",
                        "dataset của mình là gì?"
                    ]
                else:
                    # Format summary
                    largest = rows[0]
                    summary = (
                        f"**Bảng lớn nhất**: `{largest.get('schema', 'N/A')}.{largest.get('table', 'N/A')}` "
                        f"({largest.get('row_count', 0):,} dòng). "
                        f"Top {len(rows)} bảng hiển thị bên dưới."
                    )
                    
                    # Format table info
                    table_rows = []
                    for table in rows:
                        schema = table.get('schema', 'N/A')
                        table_name = table.get('table', 'N/A')
                        row_count = table.get('row_count', 0)
                        bytes_info = table.get('bytes')
                        num_files = table.get('num_files')
                        
                        row_info = f"  • `{schema}.{table_name}`: **{row_count:,}** dòng"
                        if bytes_info:
                            mb = bytes_info / (1024 * 1024)
                            row_info += f" (~{mb:.1f} MB)"
                        if num_files:
                            row_info += f" ({num_files} files)"
                        table_rows.append(row_info)
                    
                    answer = (
                        "🗂️ **Nguồn**: `lakehouse.platinum_sys.data_catalog` • "
                        "📦 Dữ liệu batch (2016–2018)\n\n"
                        f"{summary}\n\n"
                        "**📊 Top bảng theo số dòng:**\n"
                        + "\n".join(table_rows)
                    )
                    suggestions = [
                        "bảng nào lớn nhất về dung lượng?",
                        "datamart platinum nào được dùng nhiều?",
                        "dataset của mình là gì?"
                    ]
                
                log_conversation(session_id, "assistant", answer)
                
                return AskResponse(
                    session_id=session_id,
                    answer=answer,
                    sql=None,
                    rows_preview=rows if rows else None,
                    citations=None,
                    execution_time_ms=0,
                    suggestions=suggestions
                )
            except Exception as e:
                print(f"⚠️  Error getting top tables: {e}")
                answer = (
                    "⚠️ Không thể truy vấn metadata lúc này.\n\n"
                    "💡 **Gợi ý:**\n"
                    "  • Dataset của mình là gì?\n"
                    "  • Đồ án dùng công nghệ gì?\n"
                    "  • Doanh thu theo tháng gần đây?"
                )
                suggestions = [
                    "dataset của mình là gì?",
                    "đồ án dùng công nghệ gì?",
                    "doanh thu theo tháng gần đây?"
                ]
                log_conversation(session_id, "assistant", answer)
                return AskResponse(
                    session_id=session_id,
                    answer=answer,
                    sql=None,
                    rows_preview=None,
                    citations=None,
                    execution_time_ms=0,
                    suggestions=suggestions
                )
        elif topic == "about_data":
            # Use provider to get dynamic dataset info from metadata
            try:
                answer = get_about_dataset_card()
            except Exception as e:
                print(f"⚠️  Error getting dataset card: {e}")
                # Fallback to static response
                answer = (
                    "**📊 Dữ liệu TMĐT Brazil (Olist E-commerce Dataset)**\n\n"
                    "**📈 Quy mô dữ liệu:**\n"
                    "  • **Orders**: ~100,000 đơn hàng\n"
                    "  • **Products**: ~32,000 sản phẩm\n"
                    "  • **Sellers**: ~3,000 nhà bán\n"
                    "  • **Customers**: ~100,000 khách hàng\n\n"
                    "**📅 Thời gian:**\n"
                    "  • **Phạm vi**: 2016-09-04 đến 2018-10-17\n"
                    "  • **Loại**: Batch data (không realtime)\n\n"
                    "**💡 Lưu ý**: Dữ liệu batch nên số liệu ổn định, không realtime."
                )
        elif topic == "about_project":
            # Use provider to get project architecture info
            answer = get_about_project_card()
            log_conversation(session_id, "assistant", answer)
            
            return AskResponse(
                session_id=session_id,
                answer=answer,
                sql=None,
                rows_preview=None,
                citations=None,
                execution_time_ms=0,
                suggestions=suggestions_for_non_sql(topic)
            )
        
        
        else:
            answer = "Xin chào! Mình có thể giúp gì cho bạn?"
        
        # Get suggestions for non-SQL responses
        suggestions = suggestions_for_non_sql(topic)
        
        log_conversation(session_id, "assistant", answer)
        
        return AskResponse(
            session_id=session_id,
            answer=answer,
            sql=None,
            rows_preview=None,
            citations=None,
            execution_time_ms=0,
            suggestions=suggestions
        )
        
        # 1b. HELP MODE - return suggestions instead of error
        if sql_query == "":
            examples = get_example_questions()
            answer_parts = [
                "👋 **Mình có thể giúp gì cho bạn?**\n",
                "Mình là trợ lý phân tích dữ liệu Olist với các khả năng sau:\n",
                "**💡 Khả năng chính:**",
                "  • 🔍 **Truy vấn SQL thông minh**: Chuyển đổi câu hỏi tự nhiên thành SQL queries",
                "  • 📊 **Phân tích dữ liệu**: Doanh thu, sản phẩm, đơn hàng, thanh toán, logistics",
                "  • 📈 **KPI & Metrics**: GMV, AOV, retention, on-time rate, payment mix",
                "  • 🤖 **Tóm tắt kết quả**: Tự động tóm tắt insights (nếu có Gemini API)",
                "  • 📚 **RAG Citations**: Trích dẫn tài liệu khi giải thích KPI\n",
                "**🎯 Các chủ đề bạn có thể hỏi:**",
                "  • **Doanh thu & Tăng trưởng**: MoM, YoY, GMV theo thời gian/danh mục",
                "  • **Sản phẩm**: Top products, category analysis, product dimensions",
                "  • **Địa lý**: Phân bố theo bang/thành phố, regional trends",
                "  • **Seller**: KPI nhà bán, on-time rate, review scores",
                "  • **Logistics**: SLA giao hàng, delivery days, on-time rate",
                "  • **Thanh toán**: Payment mix, installments, payment trends",
                "  • **Khách hàng**: Cohort analysis, retention, customer lifecycle",
                "  • **Dự báo**: Demand forecast với confidence intervals\n",
                "**📊 Gợi ý câu hỏi phổ biến:**"
            ]
            
            for i, example in enumerate(examples[:8], 1):
                answer_parts.append(f"  {i}. {example}")
            
            answer_parts.extend([
                "\n**💬 Cách sử dụng:**",
                "  • Hỏi bằng tiếng Việt hoặc tiếng Anh",
                "  • Có thể chỉ định thời gian: \"3 tháng gần đây\", \"năm 2017\", \"Q3-2018\"",
                "  • Có thể yêu cầu Top-N: \"Top 10\", \"Top 20\"",
                "  • Có thể lọc theo danh mục, bang, seller\n",
                "**🔒 Lưu ý:**",
                "  • Chỉ truy vấn read-only (không thể INSERT/UPDATE/DELETE)",
                "  • Schema whitelist: chỉ `lakehouse.gold` và `lakehouse.platinum`",
                "  • Tự động áp dụng LIMIT và timeout để bảo vệ hiệu suất\n",
                "Hãy chọn một câu hỏi ở trên hoặc nhập câu hỏi của bạn! 🚀"
            ])
            
            answer = "\n".join(answer_parts)
            log_conversation(session_id, "assistant", answer)
            
            return AskResponse(
                session_id=session_id,
                answer=answer,
                sql=None,
                rows_preview=None,
                citations=None,
                execution_time_ms=0
            )
        
    # 1c. SQL generated - execute it (only if prefer_sql=True)
    # Only execute if sql_query is a valid SQL string (not NO_SQL, not "", not None)
    # AND user requested SQL (prefer_sql=True)
    if request.prefer_sql and sql_query and sql_query != NO_SQL and sql_query != "":
            try:
                # Parse source schema from SQL
                source_schema = _parse_schema_from_sql(sql_query)
                
                rows, exec_time = run_sql(sql_query, check_empty=False)
                # Note: total_execution_time will be calculated at the end based on start_time
                
                # Auto-enrich with product info if product_id present
                rows = enrich_with_product_info(rows)
                
                rows_preview = rows[:50]  # Preview first 50 rows
                
                # ✅ FIX: Xử lý 0 rows như output hợp lệ, không phải lỗi
                has_no_data = len(rows) == 0
                
                # Log SQL execution (with trace ID)
                log_sql_execution(session_id, sql_query, len(rows), exec_time, error=None, trace_id=trace_id)
                
                # Set no_data flag và message mềm (không raise error, vẫn trả về SQL và rows)
                if has_no_data:
                    # Tạo thông báo mềm cho 0 rows, không phải lỗi
                    no_data_msg, no_data_suggestions = message_and_suggestions(GuardCode.NO_DATA, skill_metadata, question)
                    # Lưu vào suggestions để hiển thị, nhưng KHÔNG set error_msg (vì không phải lỗi)
                    if suggestions is None:
                        suggestions = no_data_suggestions
                    # Note: answer sẽ được format với rows_preview = [] để hiển thị "Không có dữ liệu"
                
                # Update session context (Việc C - Context memory)
                # Extract time window, dimensions, measures, grain from SQL/metadata
                if skill_metadata:
                    try:
                        params = skill_metadata.get('params', {}) if isinstance(skill_metadata, dict) else {}
                        time_window = params.get('time_window', {}) if isinstance(params, dict) else {}
                        time_grain = params.get('time_grain', {}) if isinstance(params, dict) else {}
                        
                        # Format time window for storage
                        if time_window and isinstance(time_window, dict):
                            time_window_str = f"{time_window.get('start', '')}_to_{time_window.get('end', '')}"
                        else:
                            time_window_str = None
                        
                        # Extract grain (can be dict or string)
                        grain_value = None
                        if isinstance(time_grain, dict):
                            grain_value = time_grain.get('grain')
                        elif isinstance(time_grain, str):
                            grain_value = time_grain
                        
                        # Extract dimensions and measures from SQL or results
                        dimensions = []
                        measures = []
                        if rows_preview and len(rows_preview) > 0:
                            columns = list(rows_preview[0].keys())
                            # Common dimension columns
                            dim_keywords = ['category', 'state', 'seller', 'customer', 'product', 'payment', 'month', 'year', 'dt']
                            # Common measure columns
                            measure_keywords = ['gmv', 'revenue', 'orders', 'units', 'aov', 'count', 'sum', 'avg']
                            
                            for col in columns:
                                col_lower = col.lower()
                                if any(kw in col_lower for kw in dim_keywords):
                                    dimensions.append(col)
                                elif any(kw in col_lower for kw in measure_keywords):
                                    measures.append(col)
                        
                        # Update session context
                        update_session_context(
                            session_id,
                            time_window=time_window_str,
                            dimensions=dimensions[:3] if dimensions else None,
                            measures=measures[:3] if measures else None,
                            grain=grain_value
                        )
                    except Exception as e:
                        print(f"⚠️  Error updating session context: {e}")
                        # Continue without updating context
                
            except GuardError as e:
                # Handle guard errors with user-friendly messages and suggestions
                guard_code = e.code
                skill_meta = skill_metadata if skill_metadata else {}
                
                # Get message and suggestions based on error code
                error_msg, error_suggestions = message_and_suggestions(guard_code, skill_meta, question)
                
                # Store suggestions for later use
                suggestions = error_suggestions
                
                # Log the error (with trace ID)
                log_sql_execution(session_id, sql_query, 0, 0, str(e.detail), trace_id=trace_id)
                
            except HTTPException as e:
                # Fallback for HTTPException (should not happen with new code)
                error_msg = f"Lỗi SQL: {e.detail}"
                guard_code = GuardCode.AMBIGUOUS_INTENT
                suggestions = ["Doanh thu 3 tháng gần đây", "Top 10 sản phẩm bán chạy", "Phương thức thanh toán phổ biến"]
                log_sql_execution(session_id, sql_query, 0, 0, str(e.detail), trace_id=trace_id)
            except Exception as e:
                # Fallback for other exceptions
                error_msg = f"Lỗi không xác định: {str(e)}"
                guard_code = GuardCode.AMBIGUOUS_INTENT
                suggestions = ["Doanh thu 3 tháng gần đây", "Top 10 sản phẩm bán chạy", "Phương thức thanh toán phổ biến"]
                log_sql_execution(session_id, sql_query, 0, 0, str(e), trace_id=trace_id)
    
    # 1d. No SQL generated - suggest examples (only if prefer_sql=True)
    elif request.prefer_sql and (not sql_query or sql_query is None):
            # Check if this is an ambiguous intent (user wants SQL but we couldn't generate)
            # This could be due to unclear question
            guard_code = GuardCode.AMBIGUOUS_INTENT
            error_msg, suggestions = message_and_suggestions(guard_code, skill_metadata, question)
    
    # 2. RAG search (always run for context when we have SQL or need error handling)
    # Note: We already returned early for NO_SQL and HELP modes above
    if sql_query != NO_SQL and sql_query != "":
        citations = rag_search(question, k=4)
    else:
        citations = []
    
    # 3. Generate suggestions if not already set
    if suggestions is None:
        if guard_code:
            # Use guard code to generate suggestions
            suggestions = suggestions_for(skill_metadata, rows_preview, guard_code, question)
        elif rows_preview and len(rows_preview) > 0:
            # Use skill metadata and rows to generate suggestions
            suggestions = suggestions_for(skill_metadata, rows_preview, None, question)
        else:
            # Default suggestions
            suggestions = ["Doanh thu 3 tháng gần đây", "Top 10 sản phẩm bán chạy", "Phương thức thanh toán phổ biến"]
    
    # Calculate total execution time
    total_execution_time = int((time.time() - start_time) * 1000)
    
    # 4. Generate explanation if requested and enabled
    explanation = None
    if ENABLE_EXPLANATION and request.explain and sql_query and sql_query != NO_SQL and sql_query != "":
        try:
            explanation = _explain_sql_and_lineage(sql_query, source_schema, rows_preview)
        except Exception as e:
            print(f"⚠️  Explanation error: {e}")
    
    # 5. Generate quick actions if guard_code exists and enabled
    suggested_actions = None
    if ENABLE_SUGGESTED_ACTIONS and guard_code and sql_query and sql_query != NO_SQL and sql_query != "":
        try:
            issues = [guard_code] if guard_code else []
            suggested_actions = suggest_actions(sql_query, issues)
        except Exception as e:
            print(f"⚠️  Quick actions error: {e}")
    
    # 6. Format answer with header, summary, and suggestions
    # Note: We already returned early for NO_SQL and HELP modes above
    answer = format_answer(
        question=question,
        sql_query=sql_query if sql_query and sql_query != NO_SQL and sql_query != "" else None,
        rows_preview=rows_preview,
        citations=citations,
        execution_time_ms=total_execution_time,
        error=error_msg,
        source_schema=source_schema,
        suggestions=None  # Suggestions are returned separately in AskResponse
    )
    
    # Log assistant response
    log_conversation(session_id, "assistant", answer)
    
    # Log to chat_logs table for analysis and improvement
    skill_name = skill_metadata.get('skill_name') if skill_metadata else None
    confidence = skill_metadata.get('confidence') if skill_metadata else None
    error_code_str = guard_code.value if guard_code else (error_msg[:50] if error_msg else None)
    
    log_chat(
        question=question,
        generated_sql=sql_query if sql_query and sql_query != NO_SQL and sql_query != "" else None,
        error_code=error_code_str,
        execution_time_ms=total_execution_time,
        row_count=len(rows_preview) if rows_preview else 0,
        session_id=session_id,
        skill_name=skill_name,
        confidence=confidence,
        source_schema=source_schema
    )
    
    # Record metrics
    status_code = 200 if not error_msg else 500
    record_request("/ask", status_code, total_execution_time, len(rows_preview) if rows_preview else 0, error=bool(error_msg))
    
    return AskResponse(
        session_id=session_id,
        answer=answer,
        sql=sql_query if sql_query and sql_query != NO_SQL and sql_query != "" else None,
        rows_preview=rows_preview,
        citations=citations,
        execution_time_ms=total_execution_time,
        suggestions=suggestions,
        explanation=explanation,
        suggested_actions=suggested_actions
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

