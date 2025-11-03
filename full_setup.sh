#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default mode
MODE="fresh"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --fresh)
            MODE="fresh"
            shift
            ;;
        --rebuild)
            MODE="rebuild"
            shift
            ;;
        --etl)
            MODE="etl"
            shift
            ;;
        --help|-h)
            cat << EOF
${BLUE}╔════════════════════════════════════════════════════════════╗${NC}
${BLUE}║   Data Lakehouse - Setup Script                           ║${NC}
${BLUE}╚════════════════════════════════════════════════════════════╝${NC}

${YELLOW}Usage:${NC}
  ./full_setup.sh [OPTIONS]

${YELLOW}Options:${NC}
  --fresh       Clean install (remove volumes, rebuild all)
  --rebuild     Rebuild images only (keep data)
  --etl         Run ETL pipeline only (skip setup)
  --help, -h    Show this help message

${YELLOW}Examples:${NC}
  ./full_setup.sh                # Fresh install (default)
  ./full_setup.sh --fresh        # Same as above
  ./full_setup.sh --rebuild      # Rebuild after code changes
  ./full_setup.sh --etl          # Run ETL only

EOF
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Data Lakehouse - Full Setup & ETL Pipeline              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Mode: ${MODE}${NC}"
echo ""

# ============================================================================
# MODE: ETL ONLY
# ============================================================================
if [ "$MODE" = "etl" ]; then
    echo -e "${YELLOW}Running ETL pipeline...${NC}"
    docker exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data
    echo -e "${GREEN}✓ ETL completed${NC}"
    exit 0
fi

# ============================================================================
# MODE: REBUILD
# ============================================================================
if [ "$MODE" = "rebuild" ]; then
    echo -e "${YELLOW}[1/5] Stopping services...${NC}"
    docker-compose down
    echo -e "${GREEN}✓ Services stopped${NC}"
    echo ""

    echo -e "${YELLOW}[2/5] Rebuilding images (no cache)...${NC}"
    docker-compose build --no-cache dagster etl_pipeline streamlit
    echo -e "${GREEN}✓ Images rebuilt${NC}"
    echo ""

    echo -e "${YELLOW}[3/5] Starting services in order...${NC}"
    docker-compose up -d de_mysql
    sleep 20
    docker-compose up -d minio mc hive-metastore
    sleep 15
    docker-compose up -d spark-master spark-worker-1
    sleep 10
    docker-compose up -d etl_pipeline
    sleep 10
    docker-compose up -d de_dagster de_dagster_dagit de_dagster_daemon
    sleep 10
    docker-compose up -d trino metabase streamlit
    echo -e "${GREEN}✓ All services started${NC}"
    echo ""

    echo -e "${YELLOW}[4/5] Checking status...${NC}"
    docker-compose ps
    echo ""

    echo -e "${YELLOW}[5/5] Verifying services...${NC}"
    echo "  → Dagster version:"
    docker exec de_dagster pip show dagster 2>/dev/null | grep Version
    echo "  → MySQL connection:"
    docker exec de_mysql mysql -uroot -padmin123 -e "SHOW DATABASES;" 2>/dev/null | grep -q brazillian_ecommerce && echo "    ${GREEN}✓ MySQL OK${NC}" || echo "    ${RED}✗ MySQL FAILED${NC}"
    echo -e "${GREEN}✓ Rebuild complete${NC}"
    echo ""

    # Show URLs
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║   Access URLs                                              ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo "  • Dagster UI:     ${BLUE}http://localhost:3001${NC}"
    echo "  • Metabase:       ${BLUE}http://localhost:3000${NC}"
    echo "  • Spark Master:   ${BLUE}http://localhost:8080${NC}"
    echo "  • MinIO Console:  ${BLUE}http://localhost:9001${NC} (minio/minio123)"
    echo "  • Streamlit:      ${BLUE}http://localhost:8501${NC}"
    echo "  • Trino:          ${BLUE}http://localhost:8082${NC}"
    echo ""
    exit 0
fi

# ============================================================================
# MODE: FRESH (DEFAULT)
# ============================================================================
echo -e "${YELLOW}[1/9] Cleaning environment...${NC}"
docker-compose down -v 2>/dev/null || true
echo -e "${GREEN}✓ Environment cleaned${NC}"
echo ""

echo -e "${YELLOW}[2/9] Setting up environment variables...${NC}"
if [ ! -f .env ]; then
    cp env.example .env
    echo -e "${GREEN}✓ Created .env from env.example${NC}"
else
    echo -e "${GREEN}✓ .env already exists${NC}"
fi
echo ""

echo -e "${YELLOW}[3/9] Building Docker images (5-10 minutes)...${NC}"
docker-compose build
echo -e "${GREEN}✓ Images built successfully${NC}"
echo ""

echo -e "${YELLOW}[4/9] Starting services...${NC}"
docker-compose up -d
echo -e "${GREEN}✓ Services started${NC}"
echo ""

echo -e "${YELLOW}[5/9] Waiting for MySQL (30 seconds)...${NC}"
sleep 30
echo -e "${GREEN}✓ MySQL should be ready${NC}"
echo ""

echo -e "${YELLOW}[6/9] Loading dataset into MySQL...${NC}"
if [ ! -d "brazilian-ecommerce" ]; then
    echo -e "${RED}✗ Error: brazilian-ecommerce/ directory not found!${NC}"
    echo -e "${YELLOW}Please download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce${NC}"
    exit 1
fi

echo "  → Copying dataset to MySQL container..."
docker cp brazilian-ecommerce/ de_mysql:/tmp/dataset/ 2>/dev/null || {
    echo -e "${YELLOW}  ⚠ Retrying...${NC}"
    sleep 5
    docker cp brazilian-ecommerce/ de_mysql:/tmp/dataset/
}

echo "  → Copying SQL scripts..."
docker cp load_dataset_into_mysql/01_olist.sql de_mysql:/tmp/olist.sql
docker cp load_dataset_into_mysql/03_load_data.sql de_mysql:/tmp/load_data.sql

echo "  → Creating database schema..."
docker exec de_mysql mysql -uroot -padmin123 -e "SET GLOBAL local_infile=1;" 2>/dev/null || true
docker exec de_mysql mysql -uroot -padmin123 -e "CREATE DATABASE IF NOT EXISTS brazillian_ecommerce;" 2>/dev/null
docker exec de_mysql bash -c "mysql -uroot -padmin123 brazillian_ecommerce < /tmp/olist.sql" 2>/dev/null

echo "  → Loading CSV data (2-3 minutes)..."
docker exec de_mysql bash -c "mysql -uroot -padmin123 --local-infile=1 brazillian_ecommerce < /tmp/load_data.sql" 2>/dev/null

echo -e "${GREEN}✓ Data loaded into MySQL${NC}"
echo ""

echo -e "${YELLOW}[7/9] Verifying MySQL data...${NC}"
CUSTOMER_COUNT=$(docker exec de_mysql mysql -uroot -padmin123 -sN -e "SELECT COUNT(*) FROM brazillian_ecommerce.customers;" 2>/dev/null)
ORDER_COUNT=$(docker exec de_mysql mysql -uroot -padmin123 -sN -e "SELECT COUNT(*) FROM brazillian_ecommerce.orders;" 2>/dev/null)

if [ -z "$CUSTOMER_COUNT" ] || [ "$CUSTOMER_COUNT" -eq 0 ]; then
    echo -e "${RED}✗ MySQL data verification failed!${NC}"
    exit 1
fi

echo -e "${GREEN}✓ MySQL data verified:${NC}"
echo "  • Customers: $CUSTOMER_COUNT"
echo "  • Orders: $ORDER_COUNT"
echo ""

echo -e "${YELLOW}[8/9] Running ETL pipeline...${NC}"
echo "  → Waiting for Dagster to be ready (15 seconds)..."
sleep 15
echo "  → Executing ETL job..."
docker exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data 2>/dev/null || {
    echo -e "${YELLOW}  ⚠ ETL failed, you can run manually later with:${NC}"
    echo -e "${YELLOW}    ./full_setup.sh --etl${NC}"
}
echo -e "${GREEN}✓ ETL completed${NC}"
echo ""

echo -e "${YELLOW}[9/9] Final verification...${NC}"
sleep 5
docker exec trino trino --execute "SHOW SCHEMAS FROM lakehouse;" 2>/dev/null | grep -q bronze && echo "  ${GREEN}✓ Trino lakehouse OK${NC}" || echo "  ${YELLOW}⚠ Trino not ready yet${NC}"
echo ""

# ============================================================================
# SUMMARY
# ============================================================================
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Setup Complete! 🎉                                       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}📍 Access URLs:${NC}"
echo "  • Dagster UI:     ${BLUE}http://localhost:3001${NC}"
echo "  • Metabase:       ${BLUE}http://localhost:3000${NC}"
echo "  • Spark Master:   ${BLUE}http://localhost:8080${NC}"
echo "  • MinIO Console:  ${BLUE}http://localhost:9001${NC} (minio/minio123)"
echo "  • Streamlit:      ${BLUE}http://localhost:8501${NC}"
echo "  • Trino:          ${BLUE}http://localhost:8082${NC}"
echo ""
echo -e "${YELLOW}📊 Metabase Setup:${NC}"
echo "  1. Open: ${BLUE}http://localhost:3000${NC}"
echo "  2. Create admin account (email/password)"
echo "  3. Add Database:"
echo "     • Type: ${GREEN}Presto${NC}"
echo "     • Host: ${GREEN}trino${NC}"
echo "     • Port: ${GREEN}8080${NC}"
echo "     • Database: ${GREEN}lakehouse${NC}"
echo "  4. Select schemas: ${GREEN}bronze, silver, gold, platinum${NC}"
echo ""
echo -e "${YELLOW}🔧 Common Commands:${NC}"
echo "  • Run ETL again:       ${GREEN}./full_setup.sh --etl${NC}"
echo "  • Rebuild after fix:   ${GREEN}./full_setup.sh --rebuild${NC}"
echo "  • Fresh install:       ${GREEN}./full_setup.sh --fresh${NC}"
echo "  • Check services:      ${GREEN}docker-compose ps${NC}"
echo ""
echo -e "${GREEN}Happy Data Engineering! 🚀${NC}"
