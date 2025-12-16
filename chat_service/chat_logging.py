# -*- coding: utf-8 -*-
"""
Chat Logging Module
Logs chat requests, SQL generation, and query execution for analysis and improvement
"""
import os
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy import create_engine, text
from core.config import LOG_DB_URI


# Initialize log engine if configured
log_engine = None
if LOG_DB_URI:
    try:
        log_engine = create_engine(LOG_DB_URI, pool_pre_ping=True)
    except Exception as e:
        print(f"⚠️  Could not initialize log database: {e}")


def ensure_chat_logs_table():
    """
    Create chat_logs table if it doesn't exist
    """
    if not log_engine:
        return
    
    try:
        with log_engine.connect() as conn:
            # Check if table exists
            check_table_sql = """
                SELECT COUNT(*) as cnt 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = 'chat_logs'
            """
            result = conn.execute(text(check_table_sql))
            exists = result.fetchone()[0] > 0
            
            if not exists:
                # Create table
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS chat_logs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    session_id VARCHAR(64),
                    question TEXT NOT NULL,
                    generated_sql TEXT,
                    error_code VARCHAR(50),
                    execution_time_ms INT,
                    row_count INT,
                    skill_name VARCHAR(100),
                    confidence DECIMAL(5, 3),
                    source_schema VARCHAR(50),
                    INDEX idx_created_at (created_at),
                    INDEX idx_session_id (session_id),
                    INDEX idx_error_code (error_code),
                    INDEX idx_skill_name (skill_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
                conn.execute(text(create_table_sql))
                conn.commit()
                print("✅ Created chat_logs table")
            else:
                print("✅ chat_logs table already exists")
    except Exception as e:
        print(f"⚠️  Error ensuring chat_logs table: {e}")


def log_chat(
    question: str,
    generated_sql: Optional[str] = None,
    error_code: Optional[str] = None,
    execution_time_ms: Optional[int] = None,
    row_count: Optional[int] = None,
    session_id: Optional[str] = None,
    skill_name: Optional[str] = None,
    confidence: Optional[float] = None,
    source_schema: Optional[str] = None
) -> None:
    """
    Log chat request to database
    
    Args:
        question: User's question
        generated_sql: Generated SQL query (optional)
        error_code: Error code if any (optional)
        execution_time_ms: Execution time in milliseconds (optional)
        row_count: Number of rows returned (optional)
        session_id: Session ID (optional)
        skill_name: Skill name used (optional)
        confidence: Confidence score (optional)
        source_schema: Source schema (gold/platinum) (optional)
    """
    if not log_engine:
        return
    
    try:
        # Ensure table exists
        ensure_chat_logs_table()
        
        # Truncate question and SQL if too long (for database constraints)
        question_truncated = question[:5000] if len(question) > 5000 else question
        sql_truncated = generated_sql[:10000] if generated_sql and len(generated_sql) > 10000 else generated_sql
        
        with log_engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO chat_logs 
                    (session_id, question, generated_sql, error_code, execution_time_ms, 
                     row_count, skill_name, confidence, source_schema, created_at)
                    VALUES (:session_id, :question, :sql, :error_code, :exec_time, 
                            :row_count, :skill_name, :confidence, :source_schema, NOW())
                """),
                {
                    "session_id": session_id or None,
                    "question": question_truncated,
                    "sql": sql_truncated,
                    "error_code": error_code,
                    "exec_time": execution_time_ms,
                    "row_count": row_count,
                    "skill_name": skill_name,
                    "confidence": confidence,
                    "source_schema": source_schema
                }
            )
            conn.commit()
    except Exception as e:
        # Don't fail the request if logging fails
        print(f"⚠️  Error logging chat request: {e}")


def get_chat_logs_stats(days: int = 7) -> Dict[str, Any]:
    """
    Get statistics from chat logs for analysis
    
    Args:
        days: Number of days to look back
        
    Returns:
        Dictionary with statistics
    """
    if not log_engine:
        return {}
    
    try:
        with log_engine.connect() as conn:
            # Total requests
            total_result = conn.execute(
                text("""
                    SELECT COUNT(*) as cnt 
                    FROM chat_logs 
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL :days DAY)
                """),
                {"days": days}
            )
            total = total_result.fetchone()[0]
            
            # Error rate
            error_result = conn.execute(
                text("""
                    SELECT COUNT(*) as cnt 
                    FROM chat_logs 
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL :days DAY)
                    AND error_code IS NOT NULL
                """),
                {"days": days}
            )
            errors = error_result.fetchone()[0]
            
            # Top skills
            skills_result = conn.execute(
                text("""
                    SELECT skill_name, COUNT(*) as cnt 
                    FROM chat_logs 
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL :days DAY)
                    AND skill_name IS NOT NULL
                    GROUP BY skill_name
                    ORDER BY cnt DESC
                    LIMIT 10
                """),
                {"days": days}
            )
            top_skills = {row[0]: row[1] for row in skills_result.fetchall()}
            
            # Average execution time
            avg_time_result = conn.execute(
                text("""
                    SELECT AVG(execution_time_ms) as avg_time 
                    FROM chat_logs 
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL :days DAY)
                    AND execution_time_ms IS NOT NULL
                """),
                {"days": days}
            )
            avg_time = avg_time_result.fetchone()[0] or 0
            
            return {
                "total_requests": total,
                "error_count": errors,
                "error_rate": (errors / total * 100) if total > 0 else 0,
                "top_skills": top_skills,
                "avg_execution_time_ms": float(avg_time) if avg_time else 0,
                "days": days
            }
    except Exception as e:
        print(f"⚠️  Error getting chat logs stats: {e}")
        return {}


if __name__ == "__main__":
    # Test logging
    print("="*60)
    print("Testing Chat Logging")
    print("="*60)
    
    # Ensure table exists
    ensure_chat_logs_table()
    
    # Test log
    log_chat(
        question="Doanh thu theo tháng?",
        generated_sql="SELECT year_month, SUM(gmv) FROM ...",
        execution_time_ms=234,
        row_count=12,
        skill_name="RevenueTimeseriesSkill",
        confidence=0.95,
        source_schema="platinum"
    )
    
    print("✅ Test log written")
    
    # Get stats
    stats = get_chat_logs_stats(days=1)
    print(f"\n📊 Stats (last 1 day):")
    print(f"  Total requests: {stats.get('total_requests', 0)}")
    print(f"  Error rate: {stats.get('error_rate', 0):.2f}%")

