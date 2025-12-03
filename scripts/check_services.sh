#!/bin/bash
# ============================================================================
# Service Status Checker Script
# ============================================================================

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "============================================================================"
echo "🔍 CHECKING ALL SERVICES STATUS"
echo "============================================================================"
echo ""

# Function to check service health
check_service() {
    local name=$1
    local url=$2
    local port=$3
    
    echo -n "Checking $name (port $port)... "
    
    if curl -s -f --max-time 5 "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ HEALTHY${NC}"
        return 0
    else
        echo -e "${RED}❌ UNHEALTHY${NC}"
        return 1
    fi
}

# Function to check Docker container status
check_container() {
    local container=$1
    local status=$(docker ps --filter "name=$container" --format "{{.Status}}" 2>/dev/null)
    
    if [ -z "$status" ]; then
        echo -e "${RED}❌ NOT RUNNING${NC}"
        return 1
    elif echo "$status" | grep -q "healthy"; then
        echo -e "${GREEN}✅ RUNNING (healthy)${NC}"
        return 0
    elif echo "$status" | grep -q "unhealthy"; then
        echo -e "${YELLOW}⚠️  RUNNING (unhealthy)${NC}"
        return 2
    else
        echo -e "${YELLOW}⚠️  RUNNING (no healthcheck)${NC}"
        return 0
    fi
}

# 1. Docker Containers Status
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 DOCKER CONTAINERS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

services=(
    "de_mysql:MySQL Database"
    "hive-metastore:Hive Metastore"
    "minio:MinIO S3"
    "trino:Trino Query Engine"
    "metabase:Metabase BI"
    "spark-master:Spark Master"
    "spark-worker-1:Spark Worker"
    "streamlit:Streamlit App"
    "etl_pipeline:ETL Pipeline"
    "de_dagster:Dagster"
    "de_dagster_dagit:Dagster Dagit"
    "qdrant:Qdrant Vector DB"
    "chat_service:Chat Service"
)

for service_info in "${services[@]}"; do
    container="${service_info%%:*}"
    name="${service_info##*:}"
    echo -n "  $name: "
    check_container "$container"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🌐 HTTP ENDPOINTS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check HTTP endpoints
check_service "Chat Service" "http://localhost:8001/health" "8001"
check_service "Trino" "http://localhost:8082/v1/info" "8082"
check_service "Streamlit" "http://localhost:8501" "8501"
check_service "Metabase" "http://localhost:3000/api/health" "3000"
check_service "Dagster Dagit" "http://localhost:3001" "3001"
check_service "Spark Master" "http://localhost:8080" "8080"
check_service "MinIO" "http://localhost:9000/minio/health/live" "9000"

# Qdrant health check (returns empty but 200 OK)
echo -n "Checking Qdrant (port 6333)... "
if curl -s -f --max-time 5 "http://localhost:6333/collections" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ HEALTHY${NC}"
else
    echo -e "${RED}❌ UNHEALTHY${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔗 SERVICE CONNECTIONS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test Trino query
echo -n "  Trino Query Test: "
if docker exec trino trino --execute "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ OK${NC}"
else
    echo -e "${RED}❌ FAILED${NC}"
fi

# Test MySQL connection
echo -n "  MySQL Connection: "
if docker exec de_mysql mysql -uroot -padmin123 -e "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ OK${NC}"
else
    echo -e "${RED}❌ FAILED${NC}"
fi

# Test Chat Service API
echo -n "  Chat Service API: "
response=$(curl -s http://localhost:8001/health 2>/dev/null)
if echo "$response" | grep -q "healthy"; then
    echo -e "${GREEN}✅ OK${NC}"
else
    echo -e "${RED}❌ FAILED${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 SUMMARY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Count running containers
running=$(docker ps --format "{{.Names}}" | wc -l | tr -d ' ')
total=$(docker ps -a --format "{{.Names}}" | wc -l | tr -d ' ')

echo "  Running containers: $running/$total"
echo ""
echo "  Service URLs:"
echo "    • Streamlit:      http://localhost:8501"
echo "    • Metabase:       http://localhost:3000"
echo "    • Dagster Dagit:  http://localhost:3001"
echo "    • Trino:          http://localhost:8082"
echo "    • Spark Master:   http://localhost:8080"
echo "    • MinIO Console:  http://localhost:9001"
echo "    • Chat Service:   http://localhost:8001"
echo "    • Qdrant:         http://localhost:6333"
echo ""

echo "============================================================================"
echo "✅ Status check complete!"
echo "============================================================================"

