"""
Feature Engineering for Demand Forecasting
Builds time-series features from gold.factorderitem + gold.factorder
"""
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build_features(
    spark_master: str = None,
    delta_root: str = "s3a://lakehouse",
    output_table: str = "silver.forecast_features",
    start_date: str = None,
    end_date: str = None,
):
    """
    Build forecast features from gold.factorderitem + gold.factorder
    
    Args:
        spark_master: Spark master URL (default: from env SPARK_MASTER_URL)
        delta_root: Root path for Delta tables (default: s3a://lakehouse)
        output_table: Output table name (default: silver.forecast_features)
        start_date: Filter start date (format: YYYY-MM-DD)
        end_date: Filter end date (format: YYYY-MM-DD)
    
    Output schema:
        - date: DATE
        - product_id: STRING
        - region_id: STRING (customer_state for Olist)
        - revenue: DOUBLE (original scale: price + freight)
        - quantity: INT (original scale: number of items sold)
        - y_revenue: DOUBLE (log1p transformed revenue target)
        - y_quantity: DOUBLE (log1p transformed quantity target)
        - y: DOUBLE (backward compatibility: y = y_revenue)
        - lag_1, lag_7, lag_28: DOUBLE (lagged revenue features)
        - roll7, roll28: DOUBLE (rolling revenue averages)
        - qty_roll7, qty_roll28: DOUBLE (rolling quantity averages)
        - dow: INT (day of week)
        - month: INT
        - is_weekend: INT
        - price: DOUBLE
        - payment_type: STRING
        - product_category_name: STRING (for category encoding)
    """
    
    # Initialize Spark (using pre-downloaded JARs like SparkIOManager)
    spark_master = spark_master or os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    jars_dir = os.getenv("JARS_DIR", "/opt/jars")
    
    # Use same JARs as SparkIOManager (already downloaded)
    spark_jars = ",".join([
        f"file://{jars_dir}/delta-core_2.12-2.3.0.jar",
        f"file://{jars_dir}/delta-storage-2.3.0.jar",
        f"file://{jars_dir}/hadoop-aws-3.3.2.jar",
        f"file://{jars_dir}/aws-java-sdk-bundle-1.11.1026.jar",
    ])
    
    builder = (
        SparkSession.builder
        .appName("build_forecast_features")
        .master(spark_master)
        .config("spark.jars", spark_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minio123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
    
    spark = builder.getOrCreate()
    
    try:
        # Read gold tables (with underscores in table names)
        fact_order_item_path = f"{delta_root}/gold/fact_order_item"
        fact_order_path = f"{delta_root}/gold/fact_order"
        dim_customer_path = f"{delta_root}/gold/dim_customer"
        dim_product_path = f"{delta_root}/gold/dim_product"
        
        fact_order_item = spark.read.format("delta").load(fact_order_item_path)
        fact_order = spark.read.format("delta").load(fact_order_path)
        dim_customer = spark.read.format("delta").load(dim_customer_path)
        dim_product = spark.read.format("delta").load(dim_product_path)
        
        # Join to get complete picture (order_item level with payment, customer, product info)
        complete_data = (
            fact_order_item
            .join(fact_order.select("order_id", "primary_payment_type"), "order_id", "left")
            .join(dim_customer.select("customer_id", "customer_state"), "customer_id", "left")
            .join(dim_product.select("product_id", "product_category_name"), "product_id", "left")
        )
        
        # Aggregate to daily level (date × product × customer_state)
        # Targets: revenue (price + freight) and quantity (items sold)
        daily_sales = (
            complete_data
            .groupBy(
                F.col("full_date").alias("date"),
                "product_id",
                "customer_state"
            )
            .agg(
                F.sum(F.col("price") + F.col("freight_value")).alias("revenue"),  # Revenue: price + freight
                F.count("order_item_id").alias("quantity"),  # Quantity: number of items sold
                F.avg("price").alias("avg_price"),
                F.first("primary_payment_type").alias("primary_payment_type"),
                F.first("product_category_name").alias("product_category_name")  # For category encoding
            )
        )
        
        # Filter by date range if provided
        if start_date:
            daily_sales = daily_sales.filter(F.col("date") >= F.lit(start_date))
        if end_date:
            daily_sales = daily_sales.filter(F.col("date") <= F.lit(end_date))
        
        # Window for time series features per series
        w = Window.partitionBy("product_id", "customer_state").orderBy(
            F.col("date").cast("timestamp")
        )
        
        # Create features
        # Note: Keep product_category_name from aggregation - it's needed for encoding
        df = (
            daily_sales
            .withColumn("revenue", F.col("revenue").cast("double"))
            .withColumn("quantity", F.col("quantity").cast("int"))
            # Target columns (log1p transformed for training)
            .withColumn("y_revenue", F.log1p(F.col("revenue")))  # Revenue target (log1p)
            .withColumn("y_quantity", F.log1p(F.col("quantity")))  # Quantity target (log1p)
            # Backward compatibility: keep y = y_revenue for existing code
            .withColumn("y", F.col("y_revenue"))
            # Lag features for revenue
            .withColumn("lag_1", F.lag("revenue", 1).over(w))
            .withColumn("lag_7", F.lag("revenue", 7).over(w))
            .withColumn("lag_28", F.lag("revenue", 28).over(w))
            # Rolling averages for revenue
            .withColumn("roll7", F.avg("revenue").over(w.rowsBetween(-6, 0)))
            .withColumn("roll28", F.avg("revenue").over(w.rowsBetween(-27, 0)))
            # Rolling averages for quantity
            .withColumn("qty_roll7", F.avg("quantity").over(w.rowsBetween(-6, 0)))
            .withColumn("qty_roll28", F.avg("quantity").over(w.rowsBetween(-27, 0)))
            # Calendar features
            .withColumn("dow", F.dayofweek("date"))
            .withColumn("month", F.month("date"))
            .withColumn("is_weekend", F.col("dow").isin([1, 7]).cast("int"))
            .withColumnRenamed("customer_state", "region_id")
            .withColumnRenamed("avg_price", "price")
            .withColumnRenamed("primary_payment_type", "payment_type")
            # Keep product_category_name (already in daily_sales from aggregation)
        )
        
        # Select final columns - check if product_category_name exists
        base_cols = [
            "date", "product_id", "region_id",
            "revenue", "quantity",  # Original scale targets
            "y_revenue", "y_quantity",  # Log1p transformed targets
            "y",  # Backward compatibility: y = y_revenue
            "lag_1", "lag_7", "lag_28", "roll7", "roll28",
            "qty_roll7", "qty_roll28",  # Rolling quantity features
            "dow", "month", "is_weekend", "price", "payment_type"
        ]
        
        # Check if product_category_name exists in df
        df_columns = df.columns
        if "product_category_name" in df_columns:
            output_cols = base_cols + ["product_category_name"]
        else:
            # If missing, add it as null (shouldn't happen if aggregation worked)
            df = df.withColumn("product_category_name", F.lit(None).cast("string"))
            output_cols = base_cols + ["product_category_name"]
            print("⚠️  Warning: product_category_name was missing, added as null")
        
        df_final = df.select(*output_cols)
        
        # Calculate series statistics (dense vs sparse)
        # Use revenue (original scale) for statistics
        print("\n📊 Calculating series density statistics...")
        series_stats = (
            df_final
            .groupBy("product_id", "region_id")
            .agg(
                F.countDistinct("date").alias("n_days"),
                F.sum("revenue").alias("total_revenue"),  # Use original scale revenue
                F.sum("quantity").alias("total_quantity")  # Also track total quantity
            )
        )
        
        # Define thresholds for "dense" series
        MIN_DAYS = 90
        MIN_REVENUE = 1000.0
        
        series_stats = series_stats.withColumn(
            "is_dense",
            (F.col("n_days") >= MIN_DAYS) & (F.col("total_revenue") >= MIN_REVENUE)
        )
        
        # Show statistics
        stats_summary = series_stats.agg(
            F.count("*").alias("total_series"),
            F.sum(F.when(F.col("is_dense"), 1).otherwise(0)).alias("dense_series"),
            F.sum(F.when(~F.col("is_dense"), 1).otherwise(0)).alias("sparse_series")
        ).collect()[0]
        
        print(f"   Total series: {stats_summary['total_series']}")
        print(f"   Dense series (≥{MIN_DAYS} days, ≥{MIN_REVENUE} revenue): {stats_summary['dense_series']}")
        print(f"   Sparse series (will use baseline): {stats_summary['sparse_series']}")
        
        # Write features table
        output_path = f"{delta_root}/{output_table.replace('.', '/')}"
        print(f"\nWriting features to {output_path}...")
        (
            df_final
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )
        
        # Write series statistics table (for use in training and prediction)
        stats_table = output_table.replace("forecast_features", "forecast_series_stats")
        stats_path = f"{delta_root}/{stats_table.replace('.', '/')}"
        print(f"Writing series statistics to {stats_path}...")
        (
            series_stats
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(stats_path)
        )
        
        row_count = df_final.count()
        print(f"\n✅ Features built successfully: {row_count} rows written to {output_table}")
        print(f"✅ Series statistics written to {stats_table}")
        
        return output_table
        
    finally:
        spark.stop()


if __name__ == "__main__":
    # For standalone testing
    build_features()

