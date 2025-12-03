#!/bin/bash
# Service Status Check Script
# Kiểm tra tất cả các service trong docker-compose

echo "=========================================="
echo "🔍 KIỂM TRA TẤT CẢ CÁC SERVICE"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service
check_service() {
    local name=$1
    local url=$2
    local port=$3
    
    echo -n "Checking $name... "
    if curl -s -f -o /dev/null --max-time 5 "$url" 2>/dev/null; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        return 1
    fi
}

# Function to check docker container
check_container() {
    local name=$1
    echo -n "Checking container $name... "
    if docker ps --format "{{.Names}}" | grep -q "^${name}$"; then
        local status=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "no-healthcheck")
        if [ "$status" = "healthy" ]; then
            echo -e "${GREEN}✓ RUNNING (healthy)${NC}"
        elif [ "$status" = "unhealthy" ]; then
            echo -e "${YELLOW}⚠ RUNNING (unhealthy)${NC}"
        else
            echo -e "${GREEN}✓ RUNNING${NC}"
        fi
        return 0
    else
        echo -e "${RED}✗ NOT RUNNING${NC}"
        return 1
    fi
}

echo "📦 DOCKER CONTAINERS STATUS"
echo "----------------------------------------"
check_container "de_mysql"
check_container "hive-metastore"
check_container "minio"
check_container "trino"
check_container "qdrant"
check_container "chat_service"
check_container "metabase"
check_container "streamlit"
check_container "spark-master"
check_container "spark-worker-1"
check_container "etl_pipeline"
check_container "de_dagster"
check_container "de_dagster_dagit"
check_container "de_dagster_daemon"
echo ""

echo "🌐 SERVICE HEALTH CHECKS"
echo "----------------------------------------"
check_service "MySQL" "http://localhost:3306" "3306"
check_service "Trino" "http://localhost:8082/v1/info" "8082"
check_service "Qdrant" "http://localhost:6333/health" "6333"
check_service "Chat Service" "http://localhost:8001/healthz" "8001"
check_service "Metabase" "http://localhost:3000/api/health" "3000"
check_service "Streamlit" "http://localhost:8501/_stcore/health" "8501"
check_service "Spark Master UI" "http://localhost:8080" "8080"
check_service "Dagster Dagit" "http://localhost:3001" "3001"
check_service "MinIO Console" "http://localhost:9001" "9001"
echo ""

echo "📊 DETAILED STATUS"
echo "----------------------------------------"
echo "MySQL:"
docker exec de_mysql mysqladmin ping -h localhost -uroot -padmin123 2>&1 | head -1 || echo "  ⚠ Cannot connect"
echo ""

echo "Trino:"
curl -s http://localhost:8082/v1/info | python3 -m json.tool 2>/dev/null | head -5 || echo "  ⚠ Cannot connect"
echo ""

echo "Chat Service:"
curl -s http://localhost:8001/healthz | python3 -m json.tool 2>/dev/null || echo "  ⚠ Cannot connect"
echo ""

echo "Qdrant:"
curl -s http://localhost:6333/health | python3 -m json.tool 2>/dev/null || echo "  ⚠ Cannot connect"
echo ""

echo "=========================================="
echo "✅ KIỂM TRA HOÀN TẤT"
echo "=========================================="

