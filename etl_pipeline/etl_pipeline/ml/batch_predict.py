"""
Batch Prediction for Demand Forecasting
Loads trained model from MLflow and generates forecasts
Uses recursive roll-forward for multi-horizon predictions
"""
import os
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def recursive_forecast_single_series(model, feat_row, feature_cols, horizon_days=28, target_transform="log1p"):
    """
    Generate multi-horizon forecast for a single series using recursive roll-forward
    
    Args:
        model: Trained model
        feat_row: pandas Series with features for last known date
        feature_cols: List of feature column names
        horizon_days: Number of days to forecast
        target_transform: "log1p" or "none"
    
    Returns:
        List of (horizon, yhat, yhat_lo, yhat_hi) tuples
    """
    results = []
    
    # Initialize current features
    cur_features = feat_row.copy()
    
    # Store last values for rolling updates
    last_7_values = []
    last_28_values = []
    
    # Initialize with existing lags if available
    if pd.notna(cur_features.get('lag_1')):
        last_7_values.append(cur_features['lag_1'])
    if pd.notna(cur_features.get('lag_7')) and len(last_7_values) < 7:
        for _ in range(min(6, 7 - len(last_7_values))):
            last_7_values.insert(0, cur_features.get('lag_7', cur_features.get('y', 0)))
    
    for h in range(1, horizon_days + 1):
        # 1. Extract features for prediction
        X = cur_features[feature_cols].values.reshape(1, -1)
        
        # 2. Predict
        y_pred_transformed = model.predict(X)[0]
        
        # 3. Inverse transform
        if target_transform == "log1p":
            y_pred = np.expm1(y_pred_transformed)
        else:
            y_pred = y_pred_transformed
        
        # Ensure non-negative
        y_pred = max(0, y_pred)
        
        # 4. Simple prediction intervals (±15%)
        y_pred_lo = max(0, y_pred * 0.85)
        y_pred_hi = y_pred * 1.15
        
        results.append((h, y_pred, y_pred_lo, y_pred_hi))
        
        # 5. Update features for next horizon
        # Update last_7 and last_28 lists
        last_7_values.append(y_pred)
        if len(last_7_values) > 7:
            last_7_values.pop(0)
        
        last_28_values.append(y_pred)
        if len(last_28_values) > 28:
            last_28_values.pop(0)
        
        # Update lag features
        cur_features['lag_1'] = y_pred
        if len(last_7_values) >= 7:
            cur_features['lag_7'] = last_7_values[0]
        if len(last_28_values) >= 28:
            cur_features['lag_28'] = last_28_values[0]
        
        # Update rolling averages
        cur_features['roll7'] = np.mean(last_7_values)
        if len(last_28_values) > 0:
            cur_features['roll28'] = np.mean(last_28_values)
        
        # Update calendar features (advance by 1 day)
        # dow: 1=Sunday, 7=Saturday
        cur_features['dow'] = (cur_features['dow'] % 7) + 1
        cur_features['is_weekend'] = 1 if cur_features['dow'] in [1, 7] else 0
        # month: stays same or increments (simplified, doesn't handle month boundaries)
        # For production, use proper date arithmetic
    
    return results


def batch_predict(
    run_id: str,
    features_table: str = "silver.forecast_features",
    output_table: str = "platinum.demand_forecast",
    delta_root: str = "s3a://lakehouse",
    horizon_days: int = 28,
    target_transform: str = "log1p",
):
    """
    Generate batch predictions and write to platinum layer
    Uses recursive roll-forward for improved multi-horizon forecasts
    
    Args:
        run_id: MLflow run ID of the trained model
        features_table: Input features table
        output_table: Output forecast table (platinum.demand_forecast)
        delta_root: Root path for Delta tables
        horizon_days: Number of days to forecast
        target_transform: "log1p" or "none" (must match training)
    
    Output schema:
        - forecast_date: DATE (date being forecasted)
        - product_id: STRING
        - region_id: STRING
        - horizon: INT (1..28)
        - yhat: DOUBLE (point forecast)
        - yhat_lo: DOUBLE (lower bound)
        - yhat_hi: DOUBLE (upper bound)
        - model_name: STRING
        - model_version: STRING
        - run_id: STRING
        - generated_at: TIMESTAMP
    """
    
    # Setup MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb"))
    
    # Load model
    print(f"📦 Loading model from run_id: {run_id}")
    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)
    
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
        .appName("batch_predict")
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
    )
    
    spark = builder.getOrCreate()
    
    try:
        # Load features
        table_path = f"{delta_root}/{features_table.replace('.', '/')}"
        df_spark = spark.read.format("delta").load(table_path)
        
        # Convert to pandas for prediction
        # Take most recent features per series
        df_pandas = (
            df_spark
            .groupBy("product_id", "region_id")
            .agg(F.max("date").alias("last_date"))
            .join(df_spark, ["product_id", "region_id"])
            .filter(F.col("date") == F.col("last_date"))
            .toPandas()
        )
        
        print(f"📊 Generating forecasts for {len(df_pandas)} series...")
        
        # Feature columns (must match training)
        feature_cols = [
            "lag_1", "lag_7", "lag_28", "roll7", "roll28",
            "dow", "month", "is_weekend", "price"
        ]
        
        # Drop rows with missing features
        df_pandas = df_pandas.dropna(subset=feature_cols)
        
        if len(df_pandas) == 0:
            raise ValueError("No valid features found for prediction!")
        
        print(f"📈 Using recursive roll-forward for {len(df_pandas)} series × {horizon_days} horizons...")
        
        # Generate forecasts using recursive approach
        all_forecasts = []
        last_date = df_pandas["date"].max()
        
        for idx, row in df_pandas.iterrows():
            # Get recursive forecasts for this series
            series_forecasts = recursive_forecast_single_series(
                model, row, feature_cols, horizon_days, target_transform
            )
            
            # Convert to dataframe rows
            for h, yhat, yhat_lo, yhat_hi in series_forecasts:
                forecast_date = last_date + timedelta(days=h)
                all_forecasts.append({
                    "forecast_date": forecast_date,
                    "product_id": row["product_id"],
                    "region_id": row["region_id"],
                    "horizon": h,
                    "yhat": yhat,
                    "yhat_lo": yhat_lo,
                    "yhat_hi": yhat_hi,
                    "model_name": "lightgbm_global_recursive",
                    "model_version": "v1",
                    "run_id": run_id,
                    "generated_at": pd.Timestamp.utcnow(),
                })
            
            # Progress indicator
            if (idx + 1) % 100 == 0:
                print(f"   Processed {idx + 1}/{len(df_pandas)} series...")
        
        # Create final forecast dataframe
        final_forecast = pd.DataFrame(all_forecasts)
        
        print(f"✅ Generated {len(final_forecast):,} forecast rows")
        
        # Convert to Spark DataFrame and write to Delta
        sdf = spark.createDataFrame(final_forecast)
        
        output_path = f"{delta_root}/{output_table.replace('.', '/')}"
        
        print(f"💾 Writing forecasts to {output_path}...")
        (
            sdf
            .write
            .format("delta")
            .mode("append")
            .save(output_path)
        )
        
        print(f"✅ Forecasts written successfully to {output_table}")
        
        return output_table
        
    finally:
        spark.stop()


if __name__ == "__main__":
    # For standalone testing
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python batch_predict.py <run_id>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    batch_predict(run_id)
