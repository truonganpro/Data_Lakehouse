"""
Chat Service API
FastAPI service for SQL + RAG chatbot
"""
import os
import re
import time
import uuid
from typing import Optional, List, Set, Tuple
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import trino
from trino.dbapi import connect
from qdrant_client import QdrantClient
from sqlalchemy import create_engine, text

from sql_templates import intent_to_sql, get_safe_schemas, get_example_questions
from embeddings import embed_query
from llm_sql import gen_sql_with_gemini
from llm_summarize import format_answer
from router import get_router

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
    dangerous = ["DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE"]
    
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
            if attr in ['Delete', 'Drop', 'Update', 'Insert', 'Create', 'AlterTable', 'Truncate']:
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


def enforce_sql_safety(sql: str) -> str:
    """
    Enforce SQL safety constraints using AST when available
    
    Raises:
        HTTPException if SQL is unsafe
        
    Returns:
        Modified SQL with safety constraints
    """
    if not sql or not sql.strip():
        raise HTTPException(status_code=400, detail="SQL query is empty")
    
    # Must be SELECT or WITH (read-only)
    if not READONLY_PATTERN.match(sql):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT and WITH queries are allowed (read-only)"
        )
    
    # Check for dangerous keywords using AST (more accurate)
    has_dangerous, keyword = _check_dangerous_keywords_with_ast(sql)
    if has_dangerous:
        raise HTTPException(
            status_code=400,
            detail=f"Dangerous keyword '{keyword}' not allowed"
        )
    
    # Enforce LIMIT if not present
    sql_upper = sql.upper()
    if "LIMIT" not in sql_upper:
        sql = f"{sql.rstrip(';')} LIMIT {SQL_DEFAULT_LIMIT}"
    
    # Check whitelist schemas using AST (more accurate)
    schemas = _parse_sql_schemas(sql)
    has_safe_schema = bool(schemas.intersection(SQL_WHITELIST_SCHEMAS))
    
    if not has_safe_schema:
        raise HTTPException(
            status_code=400,
            detail=f"Query must use one of these schemas: {', '.join(SQL_WHITELIST_SCHEMAS)}"
        )
    
    return sql


def run_sql(sql: str, schema: str = TRINO_DEFAULT_SCHEMA) -> tuple:
    """
    Execute SQL query on Trino
    
    Args:
        sql: SQL query string
        schema: Default schema
        
    Returns:
        Tuple of (rows, execution_time_ms)
    """
    sql = enforce_sql_safety(sql)
    
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
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"SQL execution error: {str(e)}"
        )
    
    execution_time = int((time.time() - start_time) * 1000)
    
    return result, execution_time


# ============================================================================
# SQL Generation (Template + Gemini)
# ============================================================================

def build_sql(question: str) -> str | None:
    """
    Build SQL query using multi-tier approach:
    1. Check HELP mode
    2. Try old templates (backward compatibility)
    3. Try router + skills (NEW)
    4. Fallback to Gemini LLM
    
    Args:
        question: User's question
        
    Returns:
        SQL query string, empty string (HELP mode), or None
    """
    # 1. Check HELP MODE first
    sql = intent_to_sql(question)
    if sql == "":
        print(f"💡 HELP MODE triggered for: {question}")
        return ""
    
    # 2. Try old templates (for backward compatibility)
    if sql:
        print(f"✅ Using legacy template SQL for: {question}")
        return sql
    
    # 3. NEW: Try router + skills system
    try:
        router = get_router()
        sql, metadata = router.generate_sql(question, threshold=0.6)
        
        if sql:
            skill_name = metadata.get('skill_name', 'unknown')
            confidence = metadata.get('confidence', 0)
            print(f"✅ Using skill '{skill_name}' (confidence: {confidence:.2f})")
            return sql
    except Exception as e:
        print(f"⚠️  Router error: {e}")
    
    # 4. Fallback to Gemini LLM (for complex queries)
    if os.getenv("LLM_PROVIDER", "none").lower() == "gemini":
        print(f"🤖 Falling back to Gemini for: {question}")
        sql = gen_sql_with_gemini(question)
        if sql:
            return sql
    
    # 5. No SQL generated
    return None


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
    
    # 1. Generate SQL
    if request.prefer_sql:
        sql_query = build_sql(question)
        
        # 1a. HELP MODE - return suggestions instead of error
        if sql_query == "":
            examples = get_example_questions()
            answer_parts = [
                "👋 **Mình có thể giúp gì cho bạn?**\n",
                "💡 **Khả năng:**",
                "  • Truy vấn số liệu (SQL) trên lakehouse.gold & platinum",
                "  • Phân tích doanh thu, sản phẩm, đơn hàng, thanh toán",
                "  • Giải thích định nghĩa KPI từ tài liệu\n",
                "📊 **Gợi ý câu hỏi phổ biến:**"
            ]
            
            for i, example in enumerate(examples[:7], 1):
                answer_parts.append(f"  {i}. {example}")
            
            answer_parts.append("\n💬 Hãy chọn một câu hỏi hoặc nhập câu hỏi của bạn!")
            
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
        
        # 1b. SQL generated - execute it
        if sql_query:
            try:
                rows, exec_time = run_sql(sql_query)
                total_execution_time += exec_time
                rows_preview = rows[:50]  # Preview first 50 rows
                
                # Log SQL execution
                log_sql_execution(session_id, sql_query, len(rows), exec_time)
                
            except HTTPException as e:
                error_msg = f"Lỗi SQL: {e.detail}"
                log_sql_execution(session_id, sql_query, 0, 0, str(e.detail))
            except Exception as e:
                error_msg = f"Lỗi không xác định: {str(e)}"
                log_sql_execution(session_id, sql_query, 0, 0, str(e))
        
        # 1c. No SQL generated - suggest examples
        else:
            examples = get_example_questions()
            error_msg = "Mình chưa sinh được SQL an toàn cho câu hỏi này.\n\n💡 Bạn thử một trong các câu hỏi sau:\n"
            for i, example in enumerate(examples[:5], 1):
                error_msg += f"  {i}. {example}\n"
    
    # 2. RAG search (always run for context)
    citations = rag_search(question, k=4)
    
    # 3. Format answer with optional Gemini summarization
    answer = format_answer(
        question=question,
        sql_query=sql_query,
        rows_preview=rows_preview,
        citations=citations,
        execution_time_ms=total_execution_time,
        error=error_msg
    )
    
    # Log assistant response
    log_conversation(session_id, "assistant", answer)
    
    return AskResponse(
        session_id=session_id,
        answer=answer,
        sql=sql_query,
        rows_preview=rows_preview,
        citations=citations,
        execution_time_ms=total_execution_time
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

