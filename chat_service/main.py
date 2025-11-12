"""
Chat Service API
FastAPI service for SQL + RAG chatbot
"""
import os
import re
import time
import uuid
from typing import Optional, List, Set, Tuple, Dict, Union
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import trino
from trino.dbapi import connect
from qdrant_client import QdrantClient
from sqlalchemy import create_engine, text

from sql_templates import intent_to_sql, get_safe_schemas, get_example_questions, NO_SQL
from embeddings import embed_query
from llm_sql import gen_sql_with_gemini
from llm_summarize import format_answer, _parse_schema_from_sql
from router import get_router
from errors import GuardError, GuardCode
from guard_message import message_and_suggestions
from suggestions import suggestions_for, suggestions_for_non_sql
from about_dataset_provider import get_about_dataset_card
from about_project_provider import get_about_project_card
from session_store import get_session_context, update_session_context

# SQL parsing for safety
try:
    import sqlglot
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    print("⚠️  sqlglot not available, falling back to regex-based safety")


# ============================================================================
# Configuration
# ============================================================================

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
TRINO_DEFAULT_SCHEMA = os.getenv("TRINO_DEFAULT_SCHEMA", "gold")
TRINO_USER = os.getenv("TRINO_USER", "chatbot")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "")

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

SQL_WHITELIST_SCHEMAS = set(os.getenv("SQL_WHITELIST_SCHEMAS", "gold,platinum").split(","))
SQL_DEFAULT_LIMIT = int(os.getenv("SQL_DEFAULT_LIMIT", "200"))
SQL_MAX_ROWS = int(os.getenv("SQL_MAX_ROWS", "5000"))
SQL_TIMEOUT_SECS = int(os.getenv("SQL_TIMEOUT_SECS", "45"))

LOG_DB_URI = os.getenv("LOG_DB_URI", "")

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

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Models
# ============================================================================

class AskRequest(BaseModel):
    session_id: Optional[str] = None
    question: str
    prefer_sql: Optional[bool] = True


class AskResponse(BaseModel):
    session_id: str
    answer: str
    sql: Optional[str] = None
    rows_preview: Optional[List[dict]] = None
    citations: Optional[List[dict]] = None
    execution_time_ms: Optional[int] = None
    suggestions: Optional[List[str]] = None  # Context-aware suggestions


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
    
    # Check for missing time predicate on large fact tables
    if _check_missing_time_predicate(sql):
        if raise_guard_error:
            raise GuardError(GuardCode.MISSING_TIME_PRED, "Large fact tables require time predicate")
        # Don't raise error here, just warn (allow queries without time filter for small tables)
        print("⚠️  Warning: Query on large fact table without time predicate")
    
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
    
    # Check for LIMIT (outermost)
    # Note: We check before adding LIMIT to detect if user forgot it
    sql_upper = sql.upper()
    has_limit = bool(re.search(r"\bLIMIT\s+\d+\b", sql_upper))
    
    # Only enforce LIMIT if missing (but don't raise error, just add it)
    # We'll raise error only if user explicitly requests without LIMIT
    if not has_limit:
        # Add LIMIT automatically (don't raise error for this)
        sql = f"{sql.rstrip(';')} LIMIT {SQL_DEFAULT_LIMIT}"
    
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
    
    # 1. Quick check for non-SQL modes (smalltalk, about_data, about_project)
    # Use direct check instead of intent_to_sql to avoid legacy template matching
    from sql_templates import SMALLTALK_TRIGGERS, ABOUT_DATA_TRIGGERS, ABOUT_PROJECT_TRIGGERS, HELP_TRIGGERS
    
    if any(trigger in q_lower for trigger in SMALLTALK_TRIGGERS):
        # Check if it's a personal question
        is_personal = any(kw in q_lower for kw in ["bạn là ai", "tôi là ai", "bạn biết tôi", "who are you", "who am i"])
        if is_personal or not _has_data_entities_in_question(question):
            return NO_SQL, {"topic": "smalltalk"}
    
    if any(trigger in q_lower for trigger in ABOUT_DATA_TRIGGERS):
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
    
    # 4. Fallback to Gemini LLM (for complex queries)
    if os.getenv("LLM_PROVIDER", "none").lower() == "gemini":
        print(f"🤖 Falling back to Gemini for: {question}")
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
    Search for relevant documents using RAG
    
    Args:
        question: User's question
        k: Number of results to return
        
    Returns:
        List of relevant document chunks
    """
    try:
        vector = embed_query(question)
        
        hits = qdrant_client.search(
            collection_name="knowledge_base",
            query_vector=vector,
            limit=k
        )
        
        return [
            {
                "doc_id": hit.id,
                "score": hit.score,
                "text": hit.payload.get("text", "")[:500],
                "source": hit.payload.get("source", "unknown"),
            }
            for hit in hits
        ]
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


def log_sql_execution(session_id: str, sql: str, rowcount: int, duration_ms: int, error: str = None):
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


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "trino": f"{TRINO_HOST}:{TRINO_PORT}",
            "qdrant": f"{QDRANT_HOST}:{QDRANT_PORT}",
        }
    }


@app.get("/examples")
def get_examples():
    """Get example questions"""
    return {
        "examples": get_example_questions()
    }


@app.post("/ask", response_model=AskResponse)
def ask(request: AskRequest):
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
    if request.prefer_sql:
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
        
        # 1a. Non-SQL modes (smalltalk, about_data, about_project)
        if sql_query == NO_SQL:
            topic = metadata.get("topic") if metadata else "unknown"
            non_sql_metadata = metadata
            
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
        
        # 1c. SQL generated - execute it
        # Only execute if sql_query is a valid SQL string (not NO_SQL, not "", not None)
        if sql_query and sql_query != NO_SQL and sql_query != "":
            try:
                # Parse source schema from SQL
                source_schema = _parse_schema_from_sql(sql_query)
                
                rows, exec_time = run_sql(sql_query, check_empty=False)
                total_execution_time += exec_time
                
                # Auto-enrich with product info if product_id present
                rows = enrich_with_product_info(rows)
                
                rows_preview = rows[:50]  # Preview first 50 rows
                
                # Check for empty result and raise GuardError
                if len(rows) == 0:
                    raise GuardError(GuardCode.NO_DATA, "Query returned 0 rows")
                
                # Log SQL execution
                log_sql_execution(session_id, sql_query, len(rows), exec_time)
                
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
                
                # Log the error
                log_sql_execution(session_id, sql_query, 0, 0, str(e.detail))
                
            except HTTPException as e:
                # Fallback for HTTPException (should not happen with new code)
                error_msg = f"Lỗi SQL: {e.detail}"
                guard_code = GuardCode.AMBIGUOUS_INTENT
                suggestions = ["Doanh thu 3 tháng gần đây", "Top 10 sản phẩm bán chạy", "Phương thức thanh toán phổ biến"]
                log_sql_execution(session_id, sql_query, 0, 0, str(e.detail))
            except Exception as e:
                # Fallback for other exceptions
                error_msg = f"Lỗi không xác định: {str(e)}"
                guard_code = GuardCode.AMBIGUOUS_INTENT
                suggestions = ["Doanh thu 3 tháng gần đây", "Top 10 sản phẩm bán chạy", "Phương thức thanh toán phổ biến"]
                log_sql_execution(session_id, sql_query, 0, 0, str(e))
        
        # 1d. No SQL generated - suggest examples
        elif not sql_query or sql_query is None:
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
    
    # 4. Format answer with header, summary, and suggestions
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
    
    return AskResponse(
        session_id=session_id,
        answer=answer,
        sql=sql_query if sql_query and sql_query != NO_SQL and sql_query != "" else None,
        rows_preview=rows_preview,
        citations=citations,
        execution_time_ms=total_execution_time,
        suggestions=suggestions
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

