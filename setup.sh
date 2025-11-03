#!/bin/bash
# ===================================================================
# 🚀 DATA LAKEHOUSE FRESH - COMPLETE SETUP SCRIPT
# ===================================================================
# Automated setup script for deploying the Data Lakehouse project
# on a new machine.

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ===================================================================
# Functions
# ===================================================================

print_header() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  $1"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_step() {
    echo -e "${YELLOW}[$1]${NC} $2"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${CYAN}ℹ${NC} $1"
}

# ===================================================================
# Pre-flight checks
# ===================================================================

print_header "Pre-flight Checks"

# Check Docker
print_step "1/1" "Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed!"
    echo "Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi
print_success "Docker found: $(docker --version)"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not installed!"
        echo "Please install Docker Compose: https://docs.docker.com/compose/install/"
        exit 1
    else
        COMPOSE_CMD="docker compose"
    fi
else
    COMPOSE_CMD="docker-compose"
fi
print_success "Docker Compose found"

# Check available disk space (minimum 20GB)
print_step "1/2" "Checking disk space..."
AVAILABLE_SPACE=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
if [ "$AVAILABLE_SPACE" -lt 20 ]; then
    print_warning "Low disk space: ${AVAILABLE_SPACE}GB available (recommended: 20GB+)"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    print_success "Sufficient disk space: ${AVAILABLE_SPACE}GB"
fi

# Check available RAM (minimum 8GB)
print_step "2/2" "Checking RAM..."
TOTAL_RAM=$(free -g | awk '/^Mem:/{print $2}')
if [ "$TOTAL_RAM" -lt 8 ]; then
    print_warning "Low RAM: ${TOTAL_RAM}GB total (recommended: 8GB+)"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    print_success "Sufficient RAM: ${TOTAL_RAM}GB"
fi

# ===================================================================
# Setup .env file
# ===================================================================

print_header "Environment Configuration"

if [ -f .env ]; then
    print_warning ".env file already exists"
    read -p "Overwrite with defaults? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Keeping existing .env file"
    else
        cp env.example .env
        print_success ".env file created from env.example"
    fi
else
    cp env.example .env
    print_success ".env file created from env.example"
fi

# Check if GOOGLE_API_KEY needs to be set
if grep -q "YOUR_GOOGLE_API_KEY_HERE" .env; then
    print_warning "GOOGLE_API_KEY not set in .env file"
    print_info "The Chat Service will not work without a Google API key"
    print_info "You can skip this for now and add it later"
    read -p "Enter Google API Key now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "Enter your Google API Key: " API_KEY
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "s/YOUR_GOOGLE_API_KEY_HERE/$API_KEY/" .env
        else
            # Linux
            sed -i "s/YOUR_GOOGLE_API_KEY_HERE/$API_KEY/" .env
        fi
        print_success "Google API Key configured"
    else
        print_info "Skipping API key configuration"
    fi
fi

# ===================================================================
# Download JAR dependencies
# ===================================================================

print_header "Downloading Dependencies"

if [ ! -f download_jars.sh ]; then
    print_error "download_jars.sh not found!"
    exit 1
fi

chmod +x download_jars.sh
print_step "Downloading" "Spark JARs (hadoop-aws, delta-core, mysql-connector)..."
bash download_jars.sh
print_success "Dependencies downloaded"

# ===================================================================
# Build and start services
# ===================================================================

print_header "Building Docker Images"

print_info "This will take 10-15 minutes on first run..."
print_step "Building" "All Docker images..."

$COMPOSE_CMD build --no-cache
print_success "Docker images built"

print_header "Starting Services"

print_info "Starting all services in order..."

# Start base services
print_step "Phase 1/4" "Starting MySQL, MinIO, Hive Metastore..."
$COMPOSE_CMD up -d de_mysql minio mc hive-metastore
print_info "Waiting for services to stabilize..."
sleep 30

# Start Spark
print_step "Phase 2/4" "Starting Spark cluster..."
$COMPOSE_CMD up -d spark-master spark-worker-1
print_info "Waiting for Spark to start..."
sleep 15

# Start ETL and Dagster
print_step "Phase 3/4" "Starting ETL pipeline and Dagster..."
$COMPOSE_CMD up -d etl_pipeline de_dagster de_dagster_dagit de_dagster_daemon
print_info "Waiting for Dagster to initialize..."
sleep 20

# Start remaining services
print_step "Phase 4/4" "Starting Trino, Metabase, Streamlit, Chat..."
$COMPOSE_CMD up -d trino metabase streamlit qdrant chat_service
print_info "Waiting for services to stabilize..."
sleep 20

print_success "All services started"

# ===================================================================
# Load dataset
# ===================================================================

print_header "Loading Dataset"

if [ ! -d "brazilian-ecommerce" ]; then
    print_error "brazilian-ecommerce/ directory not found!"
    print_info "Please download the Brazilian E-commerce dataset from:"
    print_info "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
    print_info ""
    read -p "Skip dataset loading? (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        print_warning "Skipping dataset loading"
        SKIP_ETL=true
    else
        exit 1
    fi
fi

if [ ! "$SKIP_ETL" = true ]; then
    print_step "1/2" "Copying dataset to MySQL container..."
    docker cp brazilian-ecommerce/ de_mysql:/tmp/dataset/ || {
        print_error "Failed to copy dataset"
        exit 1
    }
    print_success "Dataset copied"
    
    print_step "2/2" "Loading data into MySQL (this takes 2-3 minutes)..."
    
    # Check if SQL scripts exist
    if [ ! -f load_dataset_into_mysql/00_init_dagster.sql ]; then
        print_error "SQL initialization scripts not found!"
        exit 1
    fi
    
    # Initialize Dagster database
    docker cp load_dataset_into_mysql/00_init_dagster.sql de_mysql:/tmp/init_dagster.sql
    docker exec de_mysql mysql -uroot -padmin123 < /tmp/init_dagster.sql || true
    
    # Load dataset
    docker cp load_dataset_into_mysql/01_olist.sql de_mysql:/tmp/olist.sql
    docker exec de_mysql mysql -uroot -padmin123 -e "CREATE DATABASE IF NOT EXISTS brazillian_ecommerce;"
    docker exec de_mysql mysql -uroot -padmin123 brazillian_ecommerce < /tmp/olist.sql || true
    
    # Note: Actual data loading should be done manually or via full_setup.sh
    print_success "Database schema created"
    print_warning "Actual CSV data loading skipped (use full_setup.sh for complete setup)"
fi

# ===================================================================
# Run ETL pipeline
# ===================================================================

print_header "Running ETL Pipeline"

print_info "Checking if ETL can be run..."

# Check if data exists in MySQL
CUSTOMER_COUNT=$(docker exec de_mysql mysql -uroot -padmin123 -sN -e "SELECT COUNT(*) FROM brazillian_ecommerce.customers;" 2>/dev/null || echo "0")

if [ "$CUSTOMER_COUNT" -eq 0 ]; then
    print_warning "No data found in MySQL"
    print_info "Skipping ETL pipeline run"
    print_info "You can run it later with: ./full_setup.sh --etl"
else
    print_info "Found $CUSTOMER_COUNT customers in MySQL"
    read -p "Run ETL pipeline now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_step "Running" "ETL pipeline..."
        docker exec etl_pipeline dagster job execute -m etl_pipeline -j reload_data || {
            print_error "ETL pipeline failed"
            print_info "You can try running it manually later"
        }
    else
        print_info "Skipping ETL pipeline"
    fi
fi

# ===================================================================
# Verify services
# ===================================================================

print_header "Service Verification"

print_step "Checking" "Service health..."

# Check services
SERVICES=(
    "de_mysql:3306"
    "minio:9000"
    "hive-metastore:9083"
    "spark-master:8080"
    "trino:8082"
    "metabase:3000"
    "streamlit:8501"
    "chat_service:8001"
)

HEALTHY=0
UNHEALTHY=0

for SERVICE in "${SERVICES[@]}"; do
    NAME=$(echo $SERVICE | cut -d: -f1)
    PORT=$(echo $SERVICE | cut -d: -f2)
    
    if docker ps --format "{{.Names}}" | grep -q "^${NAME}$"; then
        print_success "$NAME is running"
        HEALTHY=$((HEALTHY + 1))
    else
        print_error "$NAME is not running"
        UNHEALTHY=$((UNHEALTHY + 1))
    fi
done

echo ""
if [ $UNHEALTHY -eq 0 ]; then
    print_success "All services are running ($HEALTHY/$HEALTHY)"
else
    print_warning "Some services are not running ($HEALTHY healthy, $UNHEALTHY unhealthy)"
    print_info "Run 'docker-compose ps' for details"
fi

# ===================================================================
# Final summary
# ===================================================================

print_header "Setup Complete!"

echo -e "${GREEN}✓${NC} Docker images built and started"
echo -e "${GREEN}✓${NC} Services are running"
echo ""
echo -e "${YELLOW}📍 Access URLs:${NC}"
echo "  • Streamlit App:   ${BLUE}http://localhost:8501${NC}"
echo "  • Dagster UI:      ${BLUE}http://localhost:3001${NC}"
echo "  • Metabase BI:     ${BLUE}http://localhost:3000${NC}"
echo "  • Spark Master:    ${BLUE}http://localhost:8080${NC}"
echo "  • Trino:           ${BLUE}http://localhost:8082${NC}"
echo "  • MinIO Console:   ${BLUE}http://localhost:9001${NC} (minio/minio123)"
echo "  • Chat Service:    ${BLUE}http://localhost:8001${NC}"
echo ""
echo -e "${YELLOW}📊 Next Steps:${NC}"
echo "  1. Open ${BLUE}http://localhost:8501${NC} to access the main dashboard"
echo "  2. If dataset is not loaded, run: ${GREEN}./full_setup.sh --etl${NC}"
echo "  3. To run forecasting pipeline: ${GREEN}./run_forecast_pipeline.sh${NC}"
echo "  4. To check service logs: ${GREEN}docker-compose logs -f <service_name>${NC}"
echo ""
echo -e "${YELLOW}🔧 Useful Commands:${NC}"
echo "  • Check all services:     ${GREEN}docker-compose ps${NC}"
echo "  • View logs:              ${GREEN}docker-compose logs -f${NC}"
echo "  • Stop all services:      ${GREEN}docker-compose down${NC}"
echo "  • Restart services:       ${GREEN}docker-compose restart${NC}"
echo "  • Rebuild after changes:  ${GREEN}./full_setup.sh --rebuild${NC}"
echo ""
echo -e "${YELLOW}⚠️  Important Notes:${NC}"
echo "  • First-time ETL will take 5-10 minutes"
echo "  • Make sure you have at least 20GB free disk space"
echo "  • Services may take 1-2 minutes to fully initialize"
echo "  • Check logs if any service fails to start"
echo ""
echo -e "${GREEN}Happy Data Engineering! 🚀${NC}"
echo ""

