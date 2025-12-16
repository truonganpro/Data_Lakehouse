# -*- coding: utf-8 -*-
"""
Lakehouse Maintenance Assets
Compaction, VACUUM, and monitoring for Delta Lake tables
"""
from dagster import asset, AssetIn, Output, MetadataValue
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Optional
import os


# ============================================================================
# Việc A: Spark Writer Configuration
# ============================================================================

def configure_spark_for_optimal_writes(spark: SparkSession):
    """
    Configure Spark for optimal file sizes (256-512MB per file)
    Reduces small files problem
    """
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 0)  # Don't split by record count
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")  # Adjust based on cluster
    spark.conf.set("spark.sql.files.maxPartitionBytes", 512 * 1024 * 1024)  # 512MB
    
    # Delta housekeeping (don't delete logs too early)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    print("✅ Spark configured for optimal writes (256-512MB files)")


# ============================================================================
# Việc B: Compaction by Partition
# ============================================================================

def compact_delta_partition(
    spark: SparkSession,
    path: str,
    partition_col: str,
    partition_value: str,
    target_files: int = 16,
    order_cols: Optional[List[str]] = None
) -> Dict:
    """
    Compact small files in a Delta partition into fewer, larger files
    
    Args:
        spark: SparkSession
        path: Delta table path (s3a:// or local)
        partition_col: Partition column name (e.g., "year_month")
        partition_value: Partition value to compact (e.g., "2018-01")
        target_files: Target number of files per partition
        order_cols: Columns to sort within partitions (for data skipping)
        
    Returns:
        Dict with stats: num_files_before, num_files_after, bytes_saved
    """
    from delta.tables import DeltaTable
    
    order_cols = order_cols or []
    
    # Read partition
    df = (spark.read.format("delta").load(path)
          .where(F.col(partition_col) == partition_value))
    
    # Count files before
    num_files_before = df.select(F.input_file_name()).distinct().count()
    
    # Sort within partitions for data skipping (pseudo Z-order)
    if order_cols:
        df = df.sortWithinPartitions(*order_cols)
    
    # Repartition to target number of files
    df_compacted = df.repartition(target_files)
    
    # Write back using replaceWhere (only overwrites this partition)
    (df_compacted.write
     .format("delta")
     .mode("overwrite")
     .option("replaceWhere", f"{partition_col}='{partition_value}'")
     .save(path))
    
    # Count files after
    df_after = (spark.read.format("delta").load(path)
                .where(F.col(partition_col) == partition_value))
    num_files_after = df_after.select(F.input_file_name()).distinct().count()
    
    stats = {
        "partition": f"{partition_col}={partition_value}",
        "num_files_before": num_files_before,
        "num_files_after": num_files_after,
        "reduction": num_files_before - num_files_after,
        "target_files": target_files
    }
    
    print(f"✅ Compacted {partition_col}={partition_value}: {num_files_before} → {num_files_after} files")
    
    return stats


def compact_recent_partitions(
    spark: SparkSession,
    path: str,
    partition_col: str,
    months_back: int = 6,
    target_files: int = 16,
    order_cols: Optional[List[str]] = None
) -> List[Dict]:
    """
    Compact recent partitions (last N months)
    
    Args:
        spark: SparkSession
        path: Delta table path
        partition_col: Partition column (should be "year_month" or similar)
        months_back: Number of recent months to compact
        target_files: Target files per partition
        order_cols: Columns for sorting (pseudo Z-order)
        
    Returns:
        List of stats dicts for each partition
    """
    stats_list = []
    
    for i in range(months_back):
        # Calculate partition value
        if partition_col == "year_month":
            ym = (date.today().replace(day=1) - relativedelta(months=i)).strftime("%Y-%m")
            partition_value = ym
        elif partition_col == "full_date":
            # For daily partitions, compact last N days
            d = (date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
            partition_value = d
        else:
            # Generic: use current date minus i months
            ym = (date.today().replace(day=1) - relativedelta(months=i)).strftime("%Y-%m")
            partition_value = ym
        
        try:
            stats = compact_delta_partition(
                spark, path, partition_col, partition_value,
                target_files=target_files, order_cols=order_cols
            )
            stats_list.append(stats)
        except Exception as e:
            print(f"⚠️  Error compacting {partition_col}={partition_value}: {e}")
            stats_list.append({
                "partition": f"{partition_col}={partition_value}",
                "error": str(e)
            })
    
    return stats_list


# ============================================================================
# Việc C: Pseudo Z-ORDER (Cluster by hot columns)
# ============================================================================

def write_with_clustering(
    df: DataFrame,
    path: str,
    partition_col: str,
    hot_cols: List[str],
    target_partitions: int = 128
) -> None:
    """
    Write Delta table with clustering by hot columns (pseudo Z-order)
    
    Args:
        df: DataFrame to write
        path: Output path
        partition_col: Partition column (e.g., "year_month")
        hot_cols: Columns frequently used in filters (e.g., ["product_category_id", "customer_state"])
        target_partitions: Number of partitions
    """
    # Repartition by range on hot columns, then sort within partitions
    df_clustered = (df
                    .repartitionByRange(target_partitions, *hot_cols)
                    .sortWithinPartitions(*hot_cols))
    
    # Write with partitioning
    (df_clustered.write
     .format("delta")
     .mode("overwrite")
     .partitionBy(partition_col)
     .save(path))
    
    print(f"✅ Written with clustering on: {hot_cols}")


# ============================================================================
# Việc D: VACUUM
# ============================================================================

def vacuum_delta_table(
    spark: SparkSession,
    path: str,
    retain_hours: int = 168  # 7 days default
) -> Dict:
    """
    VACUUM Delta table to remove old files
    
    Args:
        spark: SparkSession
        path: Delta table path
        retain_hours: Retention period in hours (default 7 days = 168 hours)
        
    Returns:
        Dict with vacuum stats
    """
    from delta.tables import DeltaTable
    
    # Disable retention check
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    # Get Delta table
    delta_table = DeltaTable.forPath(spark, path)
    
    # Run VACUUM
    start_time = datetime.now()
    
    try:
        # VACUUM command
        spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
        
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        
        stats = {
            "path": path,
            "retain_hours": retain_hours,
            "duration_seconds": duration_seconds,
            "status": "success"
        }
        
        print(f"✅ VACUUM completed for {path} (retained {retain_hours} hours)")
        
    except Exception as e:
        stats = {
            "path": path,
            "retain_hours": retain_hours,
            "status": "error",
            "error": str(e)
        }
        print(f"❌ VACUUM failed for {path}: {e}")
    
    return stats


# ============================================================================
# Việc F: Monitor Small Files
# ============================================================================

def monitor_small_files(
    spark: SparkSession,
    path: str,
    partition_col: str = "year_month",
    min_file_size_mb: float = 64.0,
    max_files_per_partition: int = 50
) -> DataFrame:
    """
    Monitor small files in Delta table
    
    Args:
        spark: SparkSession
        path: Delta table path
        partition_col: Partition column to group by
        min_file_size_mb: Minimum file size threshold (MB)
        max_files_per_partition: Maximum files per partition threshold
        
    Returns:
        DataFrame with partition, num_files, avg_file_size_mb, needs_compaction
    """
    # Read Delta table
    df = spark.read.format("delta").load(path)
    
    # Get file names and extract partition value
    files_df = (df.select(F.input_file_name().alias("file_path"))
                .distinct())
    
    # Extract partition value from file path (e.g., year_month=2018-01)
    if partition_col == "year_month":
        files_df = files_df.withColumn(
            partition_col,
            F.regexp_extract("file_path", r"year_month=([0-9\-]+)", 1)
        )
    elif partition_col == "full_date":
        files_df = files_df.withColumn(
            partition_col,
            F.regexp_extract("file_path", r"full_date=([0-9\-]+)", 1)
        )
    else:
        # Generic: try to extract from path
        files_df = files_df.withColumn(
            partition_col,
            F.regexp_extract("file_path", f"{partition_col}=([^/]+)", 1)
        )
    
    # Group by partition and count files
    summary = (files_df
               .groupBy(partition_col)
               .agg(
                   F.count("*").alias("num_files"),
                   F.count("*").alias("file_count")  # Placeholder for avg size
               )
               .filter(F.col(partition_col) != "")  # Remove empty partitions
               .withColumn(
                   "needs_compaction",
                   (F.col("num_files") > max_files_per_partition)
               )
               .orderBy(F.col("num_files").desc()))
    
    return summary


# ============================================================================
# Dagster Assets
# ============================================================================

@asset(
    io_manager_key="spark_io_manager",
    compute_kind="SparkSQL",
    group_name="maintenance",
    description="Compact recent partitions of gold.fact_order_item"
)
def compact_gold_fact_order_item_recent_partitions(context) -> Output:
    """Compact last 6 months of fact_order_item"""
    # Get Spark session from context
    spark_io_manager = context.resources.spark_io_manager
    spark = spark_io_manager._get_spark()
    configure_spark_for_optimal_writes(spark)
    
    path = "s3a://lakehouse/gold/fact_order_item"
    partition_col = "year_month"
    
    # Hot columns for clustering (pseudo Z-order)
    order_cols = ["product_category_id", "customer_state"]
    
    stats_list = compact_recent_partitions(
        spark, path, partition_col,
        months_back=6,
        target_files=16,
        order_cols=order_cols
    )
    
    # Calculate summary
    total_before = sum(s.get("num_files_before", 0) for s in stats_list if "num_files_before" in s)
    total_after = sum(s.get("num_files_after", 0) for s in stats_list if "num_files_after" in s)
    
    context.log.info(f"Compacted {len(stats_list)} partitions: {total_before} → {total_after} files")
    
    return Output(
        value=stats_list,
        metadata={
            "partitions_compacted": len(stats_list),
            "total_files_before": total_before,
            "total_files_after": total_after,
            "files_reduced": total_before - total_after
        }
    )


@asset(
    io_manager_key="spark_io_manager",
    compute_kind="SparkSQL",
    group_name="maintenance",
    description="Compact platinum datamarts (smaller tables, fewer files needed)"
)
def compact_platinum_datamarts(context) -> Output:
    """Compact platinum datamarts (1-4 files per partition)"""
    # Get Spark session from context
    spark_io_manager = context.resources.spark_io_manager
    spark = spark_io_manager._get_spark()
    configure_spark_for_optimal_writes(spark)
    
    datamarts = [
        "s3a://lakehouse/platinum/dm_sales_monthly_category",
        "s3a://lakehouse/platinum/dm_customer_lifecycle",
        "s3a://lakehouse/platinum/dm_seller_kpi",
        "s3a://lakehouse/platinum/dm_payment_mix"
    ]
    
    all_stats = []
    
    for path in datamarts:
        try:
            # For datamarts, use fewer files (1-4 per partition)
            stats_list = compact_recent_partitions(
                spark, path, "year_month",
                months_back=3,  # Only recent 3 months
                target_files=2,  # Small tables, fewer files
                order_cols=None  # No need for complex clustering
            )
            all_stats.extend(stats_list)
        except Exception as e:
            context.log.warning(f"Error compacting {path}: {e}")
    
    return Output(
        value=all_stats,
        metadata={
            "datamarts_compacted": len(datamarts),
            "total_partitions": len(all_stats)
        }
    )


@asset(
    io_manager_key="spark_io_manager",
    compute_kind="SparkSQL",
    group_name="maintenance",
    description="VACUUM gold and platinum tables (remove old files, retain 7 days)"
)
def vacuum_gold_and_platinum(context) -> Output:
    """VACUUM gold and platinum tables"""
    # Get Spark session from context
    spark_io_manager = context.resources.spark_io_manager
    spark = spark_io_manager._get_spark()
    
    tables = [
        "s3a://lakehouse/gold/fact_order",
        "s3a://lakehouse/gold/fact_order_item",
        "s3a://lakehouse/platinum/dm_sales_monthly_category",
        "s3a://lakehouse/platinum/dm_customer_lifecycle",
        "s3a://lakehouse/platinum/dm_seller_kpi",
        "s3a://lakehouse/platinum/dm_payment_mix"
    ]
    
    vacuum_stats = []
    
    for path in tables:
        try:
            stats = vacuum_delta_table(spark, path, retain_hours=168)  # 7 days
            vacuum_stats.append(stats)
            context.log.info(f"VACUUM {path}: {stats.get('status', 'unknown')}")
        except Exception as e:
            context.log.warning(f"Error VACUUMing {path}: {e}")
            vacuum_stats.append({
                "path": path,
                "status": "error",
                "error": str(e)
            })
    
    return Output(
        value=vacuum_stats,
        metadata={
            "tables_vacuumed": len(tables),
            "successful": sum(1 for s in vacuum_stats if s.get("status") == "success")
        }
    )


@asset(
    io_manager_key="spark_io_manager",
    compute_kind="SparkSQL",
    group_name="maintenance",
    description="Monitor small files across gold and platinum tables"
)
def small_files_monitor(context) -> Output:
    """Monitor small files and identify partitions needing compaction"""
    # Get Spark session from context
    spark_io_manager = context.resources.spark_io_manager
    spark = spark_io_manager._get_spark()
    
    tables = [
        ("s3a://lakehouse/gold/fact_order", "year_month"),
        ("s3a://lakehouse/gold/fact_order_item", "year_month"),
        ("s3a://lakehouse/platinum/dm_sales_monthly_category", "year_month"),
        ("s3a://lakehouse/platinum/dm_customer_lifecycle", "year_month"),
    ]
    
    all_monitoring = []
    
    for path, partition_col in tables:
        try:
            monitor_df = monitor_small_files(
                spark, path, partition_col,
                min_file_size_mb=64.0,
                max_files_per_partition=50
            )
            
            # Collect results
            rows = monitor_df.collect()
            for row in rows:
                all_monitoring.append({
                    "table": path.split("/")[-1],
                    "partition": row[partition_col],
                    "num_files": row["num_files"],
                    "needs_compaction": row["needs_compaction"]
                })
        except Exception as e:
            context.log.warning(f"Error monitoring {path}: {e}")
    
    # Count partitions needing compaction
    needs_compaction = sum(1 for m in all_monitoring if m.get("needs_compaction", False))
    
    return Output(
        value=all_monitoring,
        metadata={
            "tables_monitored": len(tables),
            "total_partitions": len(all_monitoring),
            "partitions_needing_compaction": needs_compaction
        }
    )


# ============================================================================
# Helper: Write with optimal settings
# ============================================================================

def write_delta_optimized(
    df: DataFrame,
    path: str,
    partition_col: str,
    target_partitions: int = 128,
    hot_cols: Optional[List[str]] = None,
    mode: str = "overwrite"
):
    """
    Write Delta table with optimal settings (Việc A + C combined)
    
    Args:
        df: DataFrame to write
        path: Output path
        partition_col: Partition column
        target_partitions: Number of partitions
        hot_cols: Hot columns for clustering (pseudo Z-order)
        mode: Write mode (overwrite, append, etc.)
    """
    # Configure Spark
    configure_spark_for_optimal_writes(df.sparkSession)
    
    # Repartition and cluster if hot_cols provided
    if hot_cols:
        df = (df
              .repartitionByRange(target_partitions, *hot_cols)
              .sortWithinPartitions(*hot_cols))
    else:
        df = df.repartition(target_partitions, partition_col)
    
    # Write
    (df.write
     .format("delta")
     .mode(mode)
     .partitionBy(partition_col)
     .save(path))
    
    print(f"✅ Written {path} with optimal settings (partitions: {target_partitions})")


if __name__ == "__main__":
    # Test functions
    print("="*60)
    print("Testing Lakehouse Maintenance Functions")
    print("="*60)
    
    # Note: Requires Spark session to run
    print("✅ Functions defined. Use in Dagster assets or Spark jobs.")

