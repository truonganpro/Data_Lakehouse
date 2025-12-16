#!/bin/bash
# ===============================
#  OPTIMIZE & VACUUM FOR DELTA TABLES
# ===============================
# Author: Truong An
# Project: Data Lakehouse - Modern Data Stack
# Description: Optimize and vacuum Delta Lake tables in Gold and Platinum layers
# Usage: Run manually or via Dagster schedule

set -e

SPARK_BIN="/opt/bitnami/spark/bin/spark-sql"
S3_BASE="s3a://lakehouse"

echo "🚀 Starting Lakehouse optimization..."
echo ""

# Function to optimize and vacuum a Delta table
optimize_table() {
  local table_path=$1
  local description=$2
  
  echo "🔧 OPTIMIZING: $description → $table_path"
  
  $SPARK_BIN -e "
    OPTIMIZE delta.'$table_path';
    VACUUM delta.'$table_path' RETAIN 168 HOURS;
  " || {
    echo "⚠️  Warning: Failed to optimize $description"
    return 1
  }
  
  echo "✅ Completed: $description"
  echo ""
}

# ------------------------------------
#  GOLD LAYER
# ------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 OPTIMIZING GOLD LAYER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

optimize_table "$S3_BASE/gold/fact_order" "fact_order"
optimize_table "$S3_BASE/gold/fact_order_item" "fact_order_item"
optimize_table "$S3_BASE/gold/dim_customer" "dim_customer"
optimize_table "$S3_BASE/gold/dim_product" "dim_product"
optimize_table "$S3_BASE/gold/dim_seller" "dim_seller"

# ------------------------------------
#  PLATINUM LAYER (DATAMARTS)
# ------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💎 OPTIMIZING PLATINUM LAYER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

optimize_table "$S3_BASE/platinum/dm_sales_monthly_category" "dm_sales_monthly_category"
optimize_table "$S3_BASE/platinum/dm_seller_kpi" "dm_seller_kpi"
optimize_table "$S3_BASE/platinum/dm_customer_lifecycle" "dm_customer_lifecycle"
optimize_table "$S3_BASE/platinum/dm_payment_mix" "dm_payment_mix"
optimize_table "$S3_BASE/platinum/dm_logistics_sla" "dm_logistics_sla"
optimize_table "$S3_BASE/platinum/dm_product_bestsellers" "dm_product_bestsellers"
optimize_table "$S3_BASE/platinum/dm_category_price_bands" "dm_category_price_bands"
optimize_table "$S3_BASE/platinum/forecast_monitoring" "forecast_monitoring"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 Optimize completed successfully!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

