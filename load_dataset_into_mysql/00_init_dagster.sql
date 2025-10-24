-- ============================================
-- Initialize Dagster Database and User
-- ============================================

-- Create Dagster user if not exists
CREATE USER IF NOT EXISTS 'dagster'@'%' IDENTIFIED BY 'dagster123';

-- Create Dagster database if not exists
CREATE DATABASE IF NOT EXISTS dagster;

-- Grant all privileges to dagster user
GRANT ALL PRIVILEGES ON dagster.* TO 'dagster'@'%';

-- Also create Hive user and database for Hive Metastore
CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED BY 'hive';
CREATE DATABASE IF NOT EXISTS metastore;
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

