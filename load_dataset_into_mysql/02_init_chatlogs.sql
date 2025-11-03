-- Initialize chatlogs database for Chat Service
-- This database stores conversation history and SQL audit logs

CREATE DATABASE IF NOT EXISTS chatlogs CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE chatlogs;

-- Conversations table (session metadata)
CREATE TABLE IF NOT EXISTS conversations (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    session_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_session (session_id),
    INDEX idx_user (user_id),
    INDEX idx_started (started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Messages table (conversation history)
CREATE TABLE IF NOT EXISTS messages (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    session_id VARCHAR(64) NOT NULL,
    role ENUM('user','assistant','system') NOT NULL,
    content MEDIUMTEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_session (session_id),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- SQL audit table (SQL execution logs)
CREATE TABLE IF NOT EXISTS sql_audit (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    session_id VARCHAR(64) NOT NULL,
    generated_sql MEDIUMTEXT,
    executed_sql MEDIUMTEXT,
    is_readonly TINYINT(1) DEFAULT 1,
    catalog VARCHAR(64),
    schema_name VARCHAR(64),
    tables TEXT,
    has_limit TINYINT(1),
    rowcount INT DEFAULT 0,
    duration_ms INT DEFAULT 0,
    error TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_session (session_id),
    INDEX idx_created (created_at),
    INDEX idx_error (error(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- RAG citations table (document references)
CREATE TABLE IF NOT EXISTS rag_citations (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    session_id VARCHAR(64) NOT NULL,
    doc_id VARCHAR(128),
    score DOUBLE,
    chunk_preview TEXT,
    source_file VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_session (session_id),
    INDEX idx_doc (doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert initial test message
INSERT INTO conversations (session_id, user_id, started_at)
VALUES ('test_session', 'system', NOW());

INSERT INTO messages (session_id, role, content)
VALUES ('test_session', 'system', 'Chat service initialized successfully');

SELECT 'Chatlogs database initialized successfully' AS status;

