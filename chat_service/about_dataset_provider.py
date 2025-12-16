# -*- coding: utf-8 -*-
"""
About Dataset Provider
Fetches dataset metadata from Trino and formats it for chat responses
"""
import os
from typing import Dict, List, Optional
from datetime import datetime
import trino
from trino.dbapi import connect

# Trino connection config
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")

# Cache for metadata (5 minutes TTL)
_metadata_cache = None
_cache_timestamp = None
CACHE_TTL = 300  # 5 minutes


def get_trino_connection():
    """Get Trino connection"""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        http_scheme="http"
    )


def fetch_table_info(schema: str, table: str) -> Dict:
    """Fetch basic info about a table"""
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        
        # Get row count (approximate)
        cur.execute(f"SELECT COUNT(*) as cnt FROM {schema}.{table} LIMIT 1")
        row = cur.fetchone()
        row_count = row[0] if row else 0
        
        # Get column info
        cur.execute(f"DESCRIBE {schema}.{table}")
        columns = cur.fetchall()
        
        # Find time columns
        time_cols = []
        for col in columns:
            col_name = col[0].lower()
            col_type = col[1].lower() if len(col) > 1 else ""
            if any(t in col_type for t in ["date", "timestamp"]) or any(t in col_name for t in ["date", "time", "ts", "month"]):
                time_cols.append(col[0])
        
        cur.close()
        conn.close()
        
        return {
            "row_count": row_count,
            "columns": [col[0] for col in columns],
            "time_columns": time_cols,
            "num_columns": len(columns)
        }
    except Exception as e:
        print(f"⚠️  Error fetching table info for {schema}.{table}: {e}")
        return {
            "row_count": 0,
            "columns": [],
            "time_columns": [],
            "num_columns": 0
        }


def get_dataset_overview() -> Dict:
    """
    Get dataset overview from Trino
    Returns summary of tables, layers, and data coverage
    """
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        
        # Get all tables from gold and platinum schemas
        gold_tables = []
        platinum_tables = []
        
        # Gold tables
        try:
            cur.execute("SHOW TABLES FROM lakehouse.gold")
            gold_tables = [row[0] for row in cur.fetchall()]
        except:
            pass
        
        # Platinum tables
        try:
            cur.execute("SHOW TABLES FROM lakehouse.platinum")
            platinum_tables = [row[0] for row in cur.fetchall()]
        except:
            pass
        
        # Get info for key tables
        key_tables = {
            "gold": ["fact_order", "fact_order_item", "dim_product", "dim_customer", "dim_seller"],
            "platinum": ["dm_sales_monthly_category", "dm_customer_lifecycle", "dm_seller_kpi", "dm_logistics_sla", "dm_payment_mix"]
        }
        
        table_info = {}
        total_rows = 0
        
        # Sample key tables for row counts
        for schema, tables in [("gold", gold_tables), ("platinum", platinum_tables)]:
            for table in tables:
                if table in key_tables.get(schema, []):
                    info = fetch_table_info(f"lakehouse.{schema}", table)
                    table_info[f"{schema}.{table}"] = info
                    total_rows += info.get("row_count", 0)
        
        # Get time range from fact_order (if exists)
        time_range = {"min": None, "max": None}
        if "fact_order" in gold_tables:
            try:
                cur.execute("""
                    SELECT 
                        MIN(CAST(full_date AS DATE)) as min_date,
                        MAX(CAST(full_date AS DATE)) as max_date
                    FROM lakehouse.gold.fact_order
                """)
                row = cur.fetchone()
                if row and row[0]:
                    time_range["min"] = str(row[0])
                    time_range["max"] = str(row[1]) if row[1] else None
            except:
                pass
        
        cur.close()
        conn.close()
        
        return {
            "gold_tables": gold_tables,
            "platinum_tables": platinum_tables,
            "total_gold_tables": len(gold_tables),
            "total_platinum_tables": len(platinum_tables),
            "table_info": table_info,
            "total_rows_sample": total_rows,
            "time_range": time_range
        }
    except Exception as e:
        print(f"⚠️  Error getting dataset overview: {e}")
        return {
            "gold_tables": [],
            "platinum_tables": [],
            "total_gold_tables": 0,
            "total_platinum_tables": 0,
            "table_info": {},
            "total_rows_sample": 0,
            "time_range": {"min": None, "max": None}
        }


def get_about_dataset_card() -> str:
    """
    Generate formatted card about dataset
    """
    global _metadata_cache, _cache_timestamp
    
    # Check cache
    if _metadata_cache and _cache_timestamp:
        age = (datetime.now().timestamp() - _cache_timestamp)
        if age < CACHE_TTL:
            overview = _metadata_cache
        else:
            overview = get_dataset_overview()
            _metadata_cache = overview
            _cache_timestamp = datetime.now().timestamp()
    else:
        overview = get_dataset_overview()
        _metadata_cache = overview
        _cache_timestamp = datetime.now().timestamp()
    
    # Format response
    parts = [
        "**📊 Dữ liệu TMĐT Brazil (Olist E-commerce Dataset)**\n",
        "**📈 Quy mô dữ liệu:**",
        f"  • **Gold Layer**: {overview['total_gold_tables']} bảng (Fact & Dimension tables)",
        f"  • **Platinum Layer**: {overview['total_platinum_tables']} bảng (Pre-aggregated datamarts)",
        f"  • **Tổng mẫu**: ~{overview['total_rows_sample']:,} rows (từ các bảng chính)\n"
    ]
    
    # Time range
    if overview['time_range']['min']:
        parts.append("**📅 Thời gian:**")
        parts.append(f"  • **Phạm vi**: {overview['time_range']['min']} đến {overview['time_range']['max'] or 'N/A'}")
        parts.append("  • **Loại**: Batch data (không realtime)")
        parts.append("  • **Cập nhật**: Dữ liệu tĩnh, đã được xử lý và làm sạch\n")
    
    # Key tables
    parts.append("**🏗️ Kiến trúc Medallion (Lakehouse):**")
    parts.append("  • **Bronze**: Raw data từ CSV (chưa xử lý)")
    parts.append("  • **Silver**: Data đã làm sạch, chuẩn hóa (null handling, type casting)")
    parts.append("  • **Gold**: Fact & Dimension tables (star schema)")
    parts.append("    - `fact_order`, `fact_order_item` (measures)")
    parts.append("    - `dim_product`, `dim_customer`, `dim_seller`, `dim_geolocation`, `dim_date`")
    parts.append("  • **Platinum**: Datamarts tổng hợp (pre-aggregated)\n")
    
    # Top tables by row count
    if overview['table_info']:
        parts.append("**📦 Bảng chính (mẫu):**")
        sorted_tables = sorted(
            overview['table_info'].items(),
            key=lambda x: x[1].get('row_count', 0),
            reverse=True
        )[:5]
        
        for table_name, info in sorted_tables:
            row_count = info.get('row_count', 0)
            if row_count > 0:
                parts.append(f"  • `{table_name}`: ~{row_count:,} rows")
    
    # Datamarts
    platinum_dm = [t for t in overview['platinum_tables'] if t.startswith('dm_')]
    if platinum_dm:
        parts.append("\n**📦 Datamarts chính (Platinum layer):**")
        dm_descriptions = {
            "dm_sales_monthly_category": "Doanh thu theo danh mục/tháng (GMV, orders, units, AOV)",
            "dm_customer_lifecycle": "Phân tích cohort & retention (customers_active, retention_pct)",
            "dm_seller_kpi": "KPI nhà bán (GMV, orders, on_time_rate, cancel_rate, avg_review_score)",
            "dm_logistics_sla": "SLA giao hàng theo vùng (delivery_days_avg, on_time_rate)",
            "dm_payment_mix": "Tỷ trọng phương thức thanh toán (credit_card, boleto, voucher, debit_card)",
            "demand_forecast": "Dự báo nhu cầu (ML model với confidence intervals)"
        }
        
        for dm in platinum_dm[:6]:
            desc = dm_descriptions.get(dm, "Datamart tổng hợp")
            parts.append(f"  • `{dm}`: {desc}")
    
    parts.append("\n**💡 Lưu ý quan trọng:**")
    parts.append("  • Dữ liệu **batch** nên số liệu ổn định, không realtime")
    parts.append("  • Tất cả queries là **read-only** (chỉ SELECT, không INSERT/UPDATE/DELETE)")
    parts.append("  • Schema whitelist: chỉ truy vấn `lakehouse.gold` và `lakehouse.platinum`")
    parts.append("  • Tự động áp dụng **LIMIT** và **timeout** để bảo vệ hiệu suất")
    
    return "\n".join(parts)


def top_tables_by_rows(n: int = 5) -> List[Dict]:
    """
    Get top N tables by row count from metadata catalog
    
    Args:
        n: Number of tables to return (default 5)
        
    Returns:
        List of dicts with schema, table, row_count, bytes, num_files
    """
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        
        # Try to read from platinum_sys.data_catalog if exists
        try:
            sql = f"""
                SELECT 
                    schema, 
                    table_name as table, 
                    row_count, 
                    bytes, 
                    num_files
                FROM lakehouse.platinum_sys.data_catalog
                ORDER BY row_count DESC
                LIMIT {int(n)}
            """
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            # Convert to list of dicts
            result = [dict(zip(columns, row)) for row in rows]
            return result
        except Exception as e:
            # Fallback: query information_schema and estimate row counts
            print(f"⚠️  platinum_sys.data_catalog not available, using fallback: {e}")
            cur.close()
            conn.close()
            
            # Fallback: query tables from gold and platinum schemas
            return _fallback_top_tables_by_rows(n)
            
    except Exception as e:
        print(f"⚠️  Error getting top tables by rows: {e}")
        return []


def _fallback_top_tables_by_rows(n: int = 5) -> List[Dict]:
    """
    Fallback: Get top tables by querying information_schema and estimating row counts
    This is slower but works when platinum_sys.data_catalog doesn't exist
    """
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        
        # Get all tables from gold and platinum
        tables = []
        
        for schema in ["gold", "platinum"]:
            try:
                cur.execute(f"SHOW TABLES FROM lakehouse.{schema}")
                schema_tables = [row[0] for row in cur.fetchall()]
                
                for table in schema_tables:
                    try:
                        # Get approximate row count (LIMIT 1 to avoid full scan)
                        # Note: This is an estimate, not exact
                        cur.execute(f"""
                            SELECT COUNT(*) as cnt 
                            FROM lakehouse.{schema}.{table} 
                            LIMIT 1
                        """)
                        row = cur.fetchone()
                        row_count = row[0] if row else 0
                        
                        if row_count > 0:
                            tables.append({
                                "schema": schema,
                                "table": table,
                                "row_count": row_count,
                                "bytes": None,  # Not available without catalog
                                "num_files": None  # Not available without catalog
                            })
                    except Exception as e:
                        # Skip tables that can't be queried
                        print(f"⚠️  Skipping {schema}.{table}: {e}")
                        continue
            except Exception as e:
                print(f"⚠️  Error listing tables from {schema}: {e}")
                continue
        
        cur.close()
        conn.close()
        
        # Sort by row_count and return top N
        tables.sort(key=lambda x: x.get("row_count", 0), reverse=True)
        return tables[:n]
        
    except Exception as e:
        print(f"⚠️  Error in fallback top tables: {e}")
        return []


if __name__ == "__main__":
    # Test
    print("="*60)
    print("Testing About Dataset Provider")
    print("="*60)
    card = get_about_dataset_card()
    print(card)
    
    print("\n" + "="*60)
    print("Testing Top Tables by Rows")
    print("="*60)
    top_tables = top_tables_by_rows(5)
    for table in top_tables:
        print(f"{table.get('schema', 'N/A')}.{table.get('table', 'N/A')}: {table.get('row_count', 0):,} rows")

