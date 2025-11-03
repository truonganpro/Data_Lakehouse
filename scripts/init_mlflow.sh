#!/bin/bash
# Initialize MLflow database for Forecasting System

echo "🔧 Initializing MLflow database..."

# Create database and user
docker exec de_mysql mysql -uroot -padmin123 << MYSQL_EOF
CREATE DATABASE IF NOT EXISTS mlflowdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'mlflow'@'%' IDENTIFIED BY 'mlflow';
GRANT ALL PRIVILEGES ON mlflowdb.* TO 'mlflow'@'%';
FLUSH PRIVILEGES;
SELECT 'MLflow database created successfully' AS status;
MYSQL_EOF

echo "✅ MLflow database initialized"
echo ""
echo "📊 Verify MLflow connection:"
echo "   docker exec etl_pipeline python -c \"import mlflow; mlflow.set_tracking_uri('mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb'); print('✅ MLflow connection OK')\""
