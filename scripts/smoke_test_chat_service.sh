#!/bin/bash
# Smoke test for Chat Service
# Test health, rate limit, query, explain, and ask endpoints

set -e

CHAT_SERVICE_URL="${CHAT_SERVICE_URL:-http://localhost:8001}"

echo "🧪 Smoke Test for Chat Service"
echo "================================"
echo "URL: $CHAT_SERVICE_URL"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test 1: Health check
echo "1️⃣  Testing /healthz..."
response=$(curl -s -w "\n%{http_code}" "$CHAT_SERVICE_URL/healthz")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
    echo -e "${GREEN}✅ Health check passed (200 OK)${NC}"
    echo "Response: $body"
else
    echo -e "${RED}❌ Health check failed (HTTP $http_code)${NC}"
    exit 1
fi
echo ""

# Test 2: Rate limit (set RATE_LIMIT_MAX_REQ=3 for testing)
echo "2️⃣  Testing rate limit..."
echo "Making 5 requests (expect some 429s)..."
for i in {1..5}; do
    response=$(curl -s -w "\n%{http_code}\n%{header_rate_limit_remaining}" "$CHAT_SERVICE_URL/healthz" 2>&1)
    http_code=$(echo "$response" | tail -n2 | head -n1)
    remaining=$(echo "$response" | tail -n1)
    
    if [ "$http_code" -eq 200 ]; then
        echo -e "  Request $i: ${GREEN}200 OK${NC} (Remaining: $remaining)"
    elif [ "$http_code" -eq 429 ]; then
        echo -e "  Request $i: ${YELLOW}429 Too Many Requests${NC} (Rate limited)"
    else
        echo -e "  Request $i: ${RED}HTTP $http_code${NC}"
    fi
done
echo ""

# Test 3: /query endpoint
echo "3️⃣  Testing /query endpoint..."
response=$(curl -s -X POST "$CHAT_SERVICE_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT 1 as ok", "limit":1000, "explain":false}' \
    -w "\n%{http_code}")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
    echo -e "${GREEN}✅ /query endpoint passed (200 OK)${NC}"
    echo "Response: $body" | jq '.' 2>/dev/null || echo "Response: $body"
else
    echo -e "${RED}❌ /query endpoint failed (HTTP $http_code)${NC}"
    echo "Response: $body"
fi
echo ""

# Test 4: /query with explain
echo "4️⃣  Testing /query with explain=true..."
response=$(curl -s -X POST "$CHAT_SERVICE_URL/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT 1 as ok", "limit":1000, "explain":true}' \
    -w "\n%{http_code}")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
    echo -e "${GREEN}✅ /query with explain passed (200 OK)${NC}"
    echo "Response: $body" | jq '.' 2>/dev/null || echo "Response: $body"
else
    echo -e "${RED}❌ /query with explain failed (HTTP $http_code)${NC}"
    echo "Response: $body"
fi
echo ""

# Test 5: /explain endpoint
echo "5️⃣  Testing /explain endpoint..."
response=$(curl -s -X POST "$CHAT_SERVICE_URL/explain" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT category, SUM(gmv) FROM platinum.dm_sales_monthly_category GROUP BY 1"}' \
    -w "\n%{http_code}")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
    echo -e "${GREEN}✅ /explain endpoint passed (200 OK)${NC}"
    echo "Response: $body" | jq '.' 2>/dev/null || echo "Response: $body"
else
    echo -e "${RED}❌ /explain endpoint failed (HTTP $http_code)${NC}"
    echo "Response: $body"
fi
echo ""

# Test 6: /ask endpoint
echo "6️⃣  Testing /ask endpoint..."
response=$(curl -s -X POST "$CHAT_SERVICE_URL/ask" \
    -H "Content-Type: application/json" \
    -d '{"question":"Doanh thu theo tháng 6 tháng gần đây", "prefer_sql":true, "explain":false}' \
    -w "\n%{http_code}")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
    echo -e "${GREEN}✅ /ask endpoint passed (200 OK)${NC}"
    echo "Response: $body" | jq '.' 2>/dev/null || echo "Response: $body"
    
    # Check for suggested_actions
    if echo "$body" | jq -e '.suggested_actions' > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Suggested actions present${NC}"
    else
        echo -e "${YELLOW}⚠️  Suggested actions not present${NC}"
    fi
else
    echo -e "${RED}❌ /ask endpoint failed (HTTP $http_code)${NC}"
    echo "Response: $body"
fi
echo ""

# Test 7: /ask with explain
echo "7️⃣  Testing /ask with explain=true..."
response=$(curl -s -X POST "$CHAT_SERVICE_URL/ask" \
    -H "Content-Type: application/json" \
    -d '{"question":"Doanh thu theo tháng 6 tháng gần đây", "prefer_sql":true, "explain":true}' \
    -w "\n%{http_code}")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
    echo -e "${GREEN}✅ /ask with explain passed (200 OK)${NC}"
    echo "Response: $body" | jq '.' 2>/dev/null || echo "Response: $body"
    
    # Check for explanation
    if echo "$body" | jq -e '.explanation' > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Explanation present${NC}"
    else
        echo -e "${YELLOW}⚠️  Explanation not present${NC}"
    fi
else
    echo -e "${RED}❌ /ask with explain failed (HTTP $http_code)${NC}"
    echo "Response: $body"
fi
echo ""

echo "================================"
echo -e "${GREEN}✅ Smoke test completed!${NC}"

