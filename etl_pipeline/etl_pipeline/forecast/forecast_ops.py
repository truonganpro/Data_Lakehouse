"""
Dagster ops for Demand Forecasting Pipeline
"""
from dagster import op, Out, In, get_dagster_logger


@op(out=Out(str))
def op_build_features():
    """Build forecast features from gold.factorderitem + gold.factorder"""
    logger = get_dagster_logger()
    logger.info("🔨 Building forecast features...")
    
    from etl_pipeline.ml.feature_build import build_features
    
    table_name = build_features(
        delta_root="s3a://lakehouse",
        output_table="silver.forecast_features",
    )
    
    logger.info(f"✅ Features built: {table_name}")
    return table_name


@op(ins={"features_table": In(str)}, out=Out(str))
def op_train_model(features_table: str) -> str:
    """Train LightGBM model for revenue (default)"""
    logger = get_dagster_logger()
    logger.info("🤖 Training forecasting model (revenue)...")
    
    from etl_pipeline.ml.train_models import train_lightgbm, TargetType
    
    run_id = train_lightgbm(
        features_table=features_table,
        mlflow_experiment="demand_forecast",
        n_splits=5,
        target_type=TargetType.REVENUE,  # Train revenue model
        target_transform="log1p",
    )
    
    logger.info(f"✅ Model trained (revenue): run_id={run_id}")
    return run_id


@op(ins={"run_id": In(str), "features_table": In(str)}, out=Out(str))
def op_batch_predict(run_id: str, features_table: str) -> str:
    """Generate batch predictions for revenue (default)"""
    logger = get_dagster_logger()
    logger.info(f"🔮 Generating forecasts with model: {run_id}")
    
    from etl_pipeline.ml.batch_predict import batch_predict, TargetType
    
    output_table = batch_predict(
        run_id=run_id,
        features_table=features_table,
        output_table=None,  # Will be set to demand_forecast_revenue by default
        horizon_days=28,
        target_type=TargetType.REVENUE,  # Predict revenue
        target_transform="log1p",
    )
    
    logger.info(f"✅ Forecasts generated: {output_table}")
    return output_table


@op(ins={"forecast_table": In(str)})
def op_monitor_forecast(forecast_table: str):
    """
    Monitor forecast accuracy by comparing actuals vs forecasts (horizon=1)
    Writes results to platinum.forecast_monitoring
    """
    logger = get_dagster_logger()
    logger.info(f"📊 Monitoring skipped (no data for yesterday in 2018 dataset)")
    return
    
    import os
    from pyspark.sql import SparkSession, functions as F
    from datetime import date, timedelta
    
    # Initialize Spark
    jars_dir = os.getenv("JARS_DIR", "/opt/jars")
    spark_jars = ",".join([
        f"file://{jars_dir}/delta-core_2.12-2.3.0.jar",
        f"file://{jars_dir}/delta-storage-2.3.0.jar",
        f"file://{jars_dir}/hadoop-aws-3.3.2.jar",
        f"file://{jars_dir}/aws-java-sdk-bundle-1.11.1026.jar",
    ])
    
    builder = (
        SparkSession.builder
        .appName("monitor_forecast")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
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
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
    )
    
    spark = builder.getOrCreate()
    
    try:
        # Monitor yesterday's forecast vs actual
        yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Monitoring date: {yesterday}")
        
        # 1. Get actuals from gold.fact_order_item (with underscores)
        actuals_with_state = spark.sql(f"""
            SELECT 
                CAST(foi.full_date AS DATE) AS date,
                foi.product_id,
                dc.customer_state AS region_id,
                SUM(foi.price + foi.freight_value) AS y_actual,
                COUNT(foi.order_item_id) AS quantity_actual
            FROM gold.fact_order_item foi
            LEFT JOIN gold.dim_customer dc ON foi.customer_id = dc.customer_id
            WHERE CAST(foi.full_date AS DATE) = DATE '{yesterday}'
            GROUP BY 1, 2, 3
        """)
        
        # 2. Get forecasts (horizon=1 for yesterday)
        forecasts = spark.sql(f"""
            SELECT 
                CAST(forecast_date AS DATE) AS date,
                product_id,
                region_id,
                horizon,
                yhat,
                yhat_lo,
                yhat_hi,
                model_name,
                run_id
            FROM platinum.demand_forecast
            WHERE CAST(forecast_date AS DATE) = DATE '{yesterday}'
                AND horizon = 1
        """)
        
        # 3. Join and calculate errors
        monitoring = (
            actuals_with_state.alias("a")
            .join(forecasts.alias("f"), 
                  (F.col("a.date") == F.col("f.date")) & 
                  (F.col("a.product_id") == F.col("f.product_id")) &
                  (F.col("a.region_id") == F.col("f.region_id")),
                  "inner")
            .select(
                F.col("a.date").alias("date"),
                F.col("a.product_id").alias("product_id"),
                F.col("a.region_id").alias("region_id"),
                F.col("f.horizon").alias("horizon"),
                F.col("a.y_actual").alias("y_actual"),
                F.col("f.yhat").alias("yhat"),
                F.col("f.yhat_lo").alias("yhat_lo"),
                F.col("f.yhat_hi").alias("yhat_hi"),
                F.col("f.model_name").alias("model_name"),
                F.col("f.run_id").alias("run_id")
            )
            .withColumn("abs_error", F.abs(F.col("y_actual") - F.col("yhat")))
            .withColumn("pct_error",
                       F.when(F.col("y_actual") == 0, None)
                        .otherwise(F.abs((F.col("y_actual") - F.col("yhat")) / F.col("y_actual")) * 100))
            .withColumn("smape",
                       F.when((F.col("y_actual") + F.col("yhat")) == 0, None)
                        .otherwise(200 * F.abs(F.col("y_actual") - F.col("yhat")) / 
                                 (F.col("y_actual") + F.col("yhat"))))
        )
        
        # 4. Write to monitoring table
        row_count = monitoring.count()
        
        if row_count > 0:
            (
                monitoring
                .write
                .format("delta")
                .mode("append")
                .saveAsTable("lakehouse.platinum.forecast_monitoring")
            )
            
            # Calculate summary metrics
            summary = monitoring.agg(
                F.avg("smape").alias("avg_smape"),
                F.avg("abs_error").alias("avg_abs_error"),
                F.count("*").alias("n_series")
            ).collect()[0]
            
            logger.info(f"✅ Monitoring completed: {row_count} rows")
            logger.info(f"   Avg sMAPE: {summary['avg_smape']:.2f}%")
            logger.info(f"   Avg Abs Error: {summary['avg_abs_error']:.2f}")
            logger.info(f"   Series monitored: {summary['n_series']}")
        else:
            logger.warning(f"⚠️  No matching forecast-actual pairs found for {yesterday}")
    
    finally:
        spark.stop()

