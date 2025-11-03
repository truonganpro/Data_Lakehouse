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
        - y: DOUBLE (target: revenue)
        - quantity: INT (order items count)
        - lag_1, lag_7, lag_28: DOUBLE (lagged features)
        - roll7, roll28: DOUBLE (rolling averages)
        - dow: INT (day of week)
        - month: INT
        - is_weekend: INT
        - price: DOUBLE
        - payment_type: STRING
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
        
        fact_order_item = spark.read.format("delta").load(fact_order_item_path)
        fact_order = spark.read.format("delta").load(fact_order_path)
        dim_customer = spark.read.format("delta").load(dim_customer_path)
        
        # Join to get complete picture (order_item level with payment & customer info)
        complete_data = (
            fact_order_item
            .join(fact_order.select("order_id", "primary_payment_type"), "order_id", "left")
            .join(dim_customer.select("customer_id", "customer_state"), "customer_id", "left")
        )
        
        # Aggregate to daily level (date × product × customer_state)
        # Target: revenue (price + freight)
        daily_sales = (
            complete_data
            .groupBy(
                F.col("full_date").alias("date"),
                "product_id",
                "customer_state"
            )
            .agg(
                F.sum(F.col("price") + F.col("freight_value")).alias("y"),  # target: revenue
                F.count("order_item_id").alias("quantity"),  # items sold
                F.avg("price").alias("avg_price"),
                F.first("primary_payment_type").alias("primary_payment_type")
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
        df = (
            daily_sales
            .withColumn("y", F.col("y").cast("double"))
            .withColumn("lag_1", F.lag("y", 1).over(w))
            .withColumn("lag_7", F.lag("y", 7).over(w))
            .withColumn("lag_28", F.lag("y", 28).over(w))
            .withColumn("roll7", F.avg("y").over(w.rowsBetween(-6, 0)))
            .withColumn("roll28", F.avg("y").over(w.rowsBetween(-27, 0)))
            .withColumn("dow", F.dayofweek("date"))
            .withColumn("month", F.month("date"))
            .withColumn("is_weekend", F.col("dow").isin([1, 7]).cast("int"))
            .withColumnRenamed("customer_state", "region_id")
            .withColumnRenamed("avg_price", "price")
            .withColumnRenamed("primary_payment_type", "payment_type")
        )
        
        # Select final columns
        output_cols = [
            "date", "product_id", "region_id", "y", "quantity",
            "lag_1", "lag_7", "lag_28", "roll7", "roll28",
            "dow", "month", "is_weekend", "price", "payment_type"
        ]
        
        df_final = df.select(*output_cols)
        
        # Write to Delta
        output_path = f"{delta_root}/{output_table.replace('.', '/')}"
        print(f"Writing features to {output_path}...")
        (
            df_final
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )
        
        row_count = df_final.count()
        print(f"✅ Features built successfully: {row_count} rows written to {output_table}")
        
        return output_table
        
    finally:
        spark.stop()


if __name__ == "__main__":
    # For standalone testing
    build_features()

