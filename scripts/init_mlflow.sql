-- Initialize MLflow database in MySQL
-- Run this script to set up MLflow tracking backend

CREATE DATABASE IF NOT EXISTS mlflowdb 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'mlflow'@'%' IDENTIFIED BY 'mlflow';

GRANT ALL PRIVILEGES ON mlflowdb.* TO 'mlflow'@'%';

FLUSH PRIVILEGES;

-- Verify
USE mlflowdb;
SHOW TABLES;

SELECT 'MLflow database initialized successfully' AS status;

