"""
Batch Prediction for Demand Forecasting
Loads trained model from MLflow and generates forecasts
Uses recursive roll-forward for multi-horizon predictions
Supports both Revenue and Quantity targets
"""
import os
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from enum import Enum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class TargetType(str, Enum):
    """Target type for forecasting"""
    REVENUE = "revenue"
    QUANTITY = "quantity"


def recursive_forecast_single_series(model, feat_row, feature_cols, last_date, horizon_days=28, target_transform="log1p", q_low=None, q_high=None):
    """
    Generate multi-horizon forecast for a single series using recursive roll-forward
    Uses REAL dates to calculate calendar features (dow, month, is_weekend)
    Uses residual quantiles for Confidence Intervals (if provided)
    
    Args:
        model: Trained model
        feat_row: pandas Series with features for last known date
        feature_cols: List of feature column names
        last_date: pd.Timestamp or datetime.date - last date with real data for this series
        horizon_days: Number of days to forecast
        target_transform: "log1p" or "none"
        q_low: Lower residual quantile (10th percentile) for CI. If None, use default ±15%
        q_high: Upper residual quantile (90th percentile) for CI. If None, use default ±15%
    
    Returns:
        List of (horizon, yhat, yhat_lo, yhat_hi) tuples
    """
    results = []
    
    # Initialize current features
    cur_features = feat_row.copy()
    
    # Store last values for rolling updates (revenue and quantity)
    last_7_values = []  # For revenue rolling
    last_28_values = []  # For revenue rolling
    last_7_qty_values = []  # For quantity rolling (NEW)
    last_28_qty_values = []  # For quantity rolling (NEW)
    
    # Initialize with existing lags if available (revenue)
    if pd.notna(cur_features.get('lag_1')):
        last_7_values.append(cur_features['lag_1'])
    if pd.notna(cur_features.get('lag_7')) and len(last_7_values) < 7:
        for _ in range(min(6, 7 - len(last_7_values))):
            last_7_values.insert(0, cur_features.get('lag_7', cur_features.get('y', 0)))
    
    # Initialize quantity rolling windows if available (NEW)
    if pd.notna(cur_features.get('quantity')):
        last_7_qty_values.append(cur_features['quantity'])
    # Use qty_roll7 as approximate if quantity not available
    if len(last_7_qty_values) == 0 and pd.notna(cur_features.get('qty_roll7')):
        qty_val = cur_features.get('qty_roll7', 0)
        for _ in range(7):
            last_7_qty_values.append(qty_val)
    if pd.notna(cur_features.get('qty_roll28')):
        qty_val = cur_features.get('qty_roll28', 0)
        for _ in range(min(28, 28)):
            last_28_qty_values.append(qty_val)
    
    # Convert last_date to pd.Timestamp if needed
    current_date = pd.to_datetime(last_date)
    
    for h in range(1, horizon_days + 1):
        # 1. Calculate REAL date for this horizon step
        current_date = current_date + timedelta(days=1)
        
        # 2. Calculate calendar features from REAL date (ISO week: 1=Mon..7=Sun)
        # But model expects: 1=Sunday, 7=Saturday (US format)
        dow_iso = current_date.isoweekday()  # 1=Mon, 7=Sun
        # Convert to US format: 1=Sun, 2=Mon, ..., 7=Sat
        dow_us = (dow_iso % 7) + 1  # Mon(1)->Mon(2), Sun(7)->Sun(1), Sat(6)->Sat(7)
        
        month = current_date.month
        is_weekend = int(dow_us in [1, 7])  # Sunday or Saturday
        
        # Update calendar features in cur_features
        cur_features['dow'] = dow_us
        cur_features['month'] = month
        cur_features['is_weekend'] = is_weekend
        
        # 3. Extract features for prediction
        X = cur_features[feature_cols].values.reshape(1, -1)
        
        # 4. Predict
        y_pred_transformed = model.predict(X)[0]
        
        # 5. Inverse transform
        if target_transform == "log1p":
            y_pred = np.expm1(y_pred_transformed)
        else:
            y_pred = y_pred_transformed
        
        # Ensure non-negative
        y_pred = max(0, y_pred)
        
        # 6. Calculate Confidence Intervals using residual quantiles
        if q_low is not None and q_high is not None:
            # Use residual-based CI (from CV)
            # q_low is typically negative, q_high is typically positive
            y_pred_lo = max(0.0, y_pred + q_low)  # Add negative residual (lowers bound)
            y_pred_hi = y_pred + q_high            # Add positive residual (raises bound)
        else:
            # Fallback to fixed ±15% if quantiles not provided
            y_pred_lo = max(0, y_pred * 0.85)
            y_pred_hi = y_pred * 1.15
        
        results.append((h, y_pred, y_pred_lo, y_pred_hi))
        
        # 7. Update features for next horizon
        # Estimate quantity from revenue (approximate: qty ≈ revenue / avg_price)
        # Use current price if available, otherwise estimate from y_pred
        if 'price' in cur_features and pd.notna(cur_features['price']) and cur_features['price'] > 0:
            estimated_qty = max(1, y_pred / cur_features['price'])  # At least 1 item
        else:
            # Fallback: use historical quantity rolling average or estimate from y_pred
            estimated_qty = np.mean(last_7_qty_values) if len(last_7_qty_values) > 0 else max(1, y_pred / 50.0)
        
        # Update revenue rolling windows
        last_7_values.append(y_pred)
        if len(last_7_values) > 7:
            last_7_values.pop(0)
        
        last_28_values.append(y_pred)
        if len(last_28_values) > 28:
            last_28_values.pop(0)
        
        # Update quantity rolling windows (NEW)
        last_7_qty_values.append(estimated_qty)
        if len(last_7_qty_values) > 7:
            last_7_qty_values.pop(0)
        
        last_28_qty_values.append(estimated_qty)
        if len(last_28_qty_values) > 28:
            last_28_qty_values.pop(0)
        
        # Update lag features (revenue)
        cur_features['lag_1'] = y_pred
        if len(last_7_values) >= 7:
            cur_features['lag_7'] = last_7_values[0]
        if len(last_28_values) >= 28:
            cur_features['lag_28'] = last_28_values[0]
        
        # Update rolling averages (revenue)
        cur_features['roll7'] = np.mean(last_7_values) if len(last_7_values) > 0 else y_pred
        if len(last_28_values) > 0:
            cur_features['roll28'] = np.mean(last_28_values)
        
        # Update rolling averages (quantity) - NEW
        cur_features['qty_roll7'] = np.mean(last_7_qty_values) if len(last_7_qty_values) > 0 else estimated_qty
        cur_features['qty_roll28'] = np.mean(last_28_qty_values) if len(last_28_qty_values) > 0 else estimated_qty
        
        # Note: payment_type_te and category_te remain constant (from initial row)
    
    return results


def baseline_forecast_single_series(
    history_df: pd.DataFrame,
    product_id: str,
    region_id: str,
    last_date: pd.Timestamp,
    horizon_days: int = 28,
    target_transform: str = "log1p",
):
    """
    Baseline forecast for sparse series using last 28-day average.
    
    Args:
        history_df: Full history dataframe with columns [date, product_id, region_id, y]
        product_id: Product ID
        region_id: Region ID
        last_date: Last date in history
        horizon_days: Forecast horizon
        target_transform: "log1p" or "none" (to inverse transform y)
    
    Returns:
        List of tuples: [(horizon, yhat, yhat_lo, yhat_hi), ...]
    """
    # Filter history for this series
    series_history = history_df[
        (history_df["product_id"] == product_id) &
        (history_df["region_id"] == region_id)
    ].sort_values("date")
    
    if len(series_history) == 0:
        # No history: return zeros
        return [(h, 0.0, 0.0, 0.0) for h in range(1, horizon_days + 1)]
    
    # Get original scale revenue (y_orig)
    if "y_orig" in series_history.columns:
        y_orig = series_history["y_orig"].values
    elif target_transform == "log1p":
        # Inverse transform from log1p
        y_orig = np.expm1(series_history["y"].values)
    else:
        y_orig = series_history["y"].values
    
    # Take last 28 days (or all available if less)
    tail = y_orig[-28:] if len(y_orig) >= 28 else y_orig
    
    if len(tail) == 0:
        # No data: return zeros
        return [(h, 0.0, 0.0, 0.0) for h in range(1, horizon_days + 1)]
    
    # Baseline level: mean of last 28 days
    baseline_level = float(np.mean(tail))
    baseline_std = float(np.std(tail)) if len(tail) > 1 else baseline_level * 0.15
    
    # For all horizons, use same baseline level
    # CI: ±1.5 * std (wider than normal because sparse series are more uncertain)
    results = []
    for h in range(1, horizon_days + 1):
        yhat = max(0.0, baseline_level)
        yhat_lo = max(0.0, yhat - 1.5 * baseline_std)
        yhat_hi = yhat + 1.5 * baseline_std
        
        results.append((h, yhat, yhat_lo, yhat_hi))
    
    return results


def batch_predict(
    run_id: str,
    features_table: str = "silver.forecast_features",
    output_table: str = None,  # Will be set based on target_type if None
    delta_root: str = "s3a://lakehouse",
    horizon_days: int = 28,
    target_type: TargetType = TargetType.REVENUE,  # "revenue" or "quantity"
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
    
    # Set output table based on target_type if not provided
    if output_table is None:
        if target_type == TargetType.REVENUE:
            output_table = "platinum.demand_forecast_revenue"
        else:
            output_table = "platinum.demand_forecast_quantity"
    
    print(f"\n📊 Batch Prediction Configuration:")
    print(f"   Target type: {target_type.value}")
    print(f"   Output table: {output_table}")
    print(f"   Horizon: {horizon_days} days")
    
    # Setup MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb"))
    
    # Load model
    print(f"\n📦 Loading model from run_id: {run_id}")
    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)
    
    # Load CI quantiles and target encodings from MLflow run
    print(f"📊 Loading CI quantiles and target encodings from run {run_id}...")
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)
    
    # Verify run has matching target_type
    run_target_type = run.data.params.get("target_type")
    if run_target_type and run_target_type != target_type.value:
        print(f"   ⚠️  WARNING: Run target_type is '{run_target_type}' but requested '{target_type.value}'")
        print(f"   ⚠️  Proceeding with requested target_type: {target_type.value}")
    
    # Get residual quantiles for CI (fallback to ±15% if not found)
    q_low = run.data.params.get("ci_residual_q10")
    q_high = run.data.params.get("ci_residual_q90")
    
    if q_low is not None and q_high is not None:
        q_low = float(q_low)
        q_high = float(q_high)
        print(f"   ✅ Using residual-based CI: q10={q_low:.2f}, q90={q_high:.2f}")
    else:
        # Fallback to fixed ±15% if quantiles not found (backward compatibility)
        print(f"   ⚠️  CI quantiles not found in run, using default ±15%")
        q_low = None  # Will use default in recursive_forecast_single_series
        q_high = None
    
    # Load target encodings (payment_type_te, category_te)
    import json
    pay_stats = {}
    cat_stats = {}
    global_mean_y = None
    
    try:
        # Get global mean for fallback
        global_mean_y = run.data.params.get("encoding_global_mean")
        if global_mean_y:
            global_mean_y = float(global_mean_y)
    except:
        pass
    
    try:
        # Load payment_type encoding
        pay_art = client.download_artifacts(run_id, "payment_type_te.json")
        with open(pay_art, 'r') as f:
            pay_stats = json.load(f)
        print(f"   ✅ Loaded payment_type_te encoding ({len(pay_stats)} categories)")
    except Exception as e:
        print(f"   ⚠️  payment_type_te encoding not found: {e}")
    
    try:
        # Load category encoding
        cat_art = client.download_artifacts(run_id, "category_te.json")
        with open(cat_art, 'r') as f:
            cat_stats = json.load(f)
        print(f"   ✅ Loaded category_te encoding ({len(cat_stats)} categories)")
    except Exception as e:
        print(f"   ⚠️  category_te encoding not found: {e}")
    
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
        
        # Load series statistics (dense vs sparse flags)
        stats_table = features_table.replace("forecast_features", "forecast_series_stats")
        stats_path = f"{delta_root}/{stats_table.replace('.', '/')}"
        
        print(f"📊 Loading series statistics from {stats_table}...")
        try:
            df_stats_spark = spark.read.format("delta").load(stats_path)
            df_stats = df_stats_spark.toPandas()
            
            # Create lookup dictionary for is_dense flag
            series_dense_map = dict(
                zip(
                    zip(df_stats["product_id"], df_stats["region_id"]),
                    df_stats["is_dense"]
                )
            )
            
            n_dense_total = df_stats["is_dense"].sum()
            n_sparse_total = (~df_stats["is_dense"]).sum()
            print(f"   ✅ Loaded series stats:")
            print(f"      Total dense series: {n_dense_total}")
            print(f"      Total sparse series: {n_sparse_total}")
        except Exception as e:
            print(f"   ⚠️  Could not load series stats: {e}")
            print(f"   ⚠️  Will use model for ALL series (no baseline)")
            series_dense_map = {}
        
        # Load full history for baseline forecast (sparse series need history)
        # Select appropriate target column based on target_type
        if target_type == TargetType.REVENUE:
            target_col = "revenue" if "revenue" in df_spark.columns else "y"
            target_col_transformed = "y_revenue" if "y_revenue" in df_spark.columns else "y"
        else:  # QUANTITY
            target_col = "quantity"
            target_col_transformed = "y_quantity" if "y_quantity" in df_spark.columns else None
        
        # Select columns for history
        history_cols = ["date", "product_id", "region_id"]
        if target_col in df_spark.columns:
            history_cols.append(target_col)
        if target_col_transformed and target_col_transformed in df_spark.columns:
            history_cols.append(target_col_transformed)
        elif "y" in df_spark.columns:
            history_cols.append("y")  # Fallback
        
        df_history_spark = df_spark.select(*history_cols).orderBy("product_id", "region_id", "date")
        df_history = df_history_spark.toPandas()
        
        # Add y_orig column (original scale target)
        if target_col in df_history.columns:
            df_history["y_orig"] = df_history[target_col]
        elif target_col_transformed and target_col_transformed in df_history.columns:
            # Inverse transform if needed
            if target_transform == "log1p":
                df_history["y_orig"] = np.expm1(df_history[target_col_transformed])
            else:
                df_history["y_orig"] = df_history[target_col_transformed]
        elif "y" in df_history.columns:
            # Fallback: inverse transform y
            if target_transform == "log1p":
                df_history["y_orig"] = np.expm1(df_history["y"])
            else:
                df_history["y_orig"] = df_history["y"]
        else:
            raise ValueError(f"Could not find target column for {target_type.value}. Available columns: {df_history.columns.tolist()}")
        
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
        
        # Apply target encodings to features
        if pay_stats and "payment_type" in df_pandas.columns:
            df_pandas["payment_type_te"] = df_pandas["payment_type"].map(pay_stats)
            if global_mean_y is not None:
                df_pandas["payment_type_te"] = df_pandas["payment_type_te"].fillna(global_mean_y)
            else:
                df_pandas["payment_type_te"] = df_pandas["payment_type_te"].fillna(df_pandas["y"].mean() if "y" in df_pandas.columns else 0)
        elif "payment_type" in df_pandas.columns:
            # Fallback: use global mean or 0
            df_pandas["payment_type_te"] = global_mean_y if global_mean_y else 0
            print(f"   ⚠️  Using fallback for payment_type_te")
        else:
            df_pandas["payment_type_te"] = global_mean_y if global_mean_y else 0
            print(f"   ⚠️  payment_type column not found, using fallback")
        
        if cat_stats and "product_category_name" in df_pandas.columns:
            df_pandas["category_te"] = df_pandas["product_category_name"].map(cat_stats)
            if global_mean_y is not None:
                df_pandas["category_te"] = df_pandas["category_te"].fillna(global_mean_y)
            else:
                df_pandas["category_te"] = df_pandas["category_te"].fillna(df_pandas["y"].mean() if "y" in df_pandas.columns else 0)
        elif "product_category_name" in df_pandas.columns:
            df_pandas["category_te"] = global_mean_y if global_mean_y else 0
            print(f"   ⚠️  Using fallback for category_te")
        else:
            df_pandas["category_te"] = global_mean_y if global_mean_y else 0
            print(f"   ⚠️  product_category_name column not found, using fallback")
        
        # Feature columns (must match training)
        feature_cols = [
            "lag_1", "lag_7", "lag_28", "roll7", "roll28",
            "qty_roll7", "qty_roll28",  # NEW: Rolling quantity features
            "dow", "month", "is_weekend", "price",
            "payment_type_te",  # NEW: Payment type encoding
            "category_te",  # NEW: Category encoding
        ]
        
        # Drop rows with missing features
        df_pandas = df_pandas.dropna(subset=feature_cols)
        
        if len(df_pandas) == 0:
            raise ValueError("No valid features found for prediction!")
        
        print(f"📈 Using recursive roll-forward for {len(df_pandas)} series × {horizon_days} horizons...")
        print(f"   Using REAL dates for calendar features (dow, month, is_weekend)")
        print(f"   Features: {len(feature_cols)} columns (including qty_roll7/28, payment_type_te, category_te)")
        
        # Generate forecasts using recursive approach (dense) or baseline (sparse)
        all_forecasts = []
        n_dense_forecast = 0
        n_sparse_forecast = 0
        
        for idx, row in df_pandas.iterrows():
            # Get last_date for THIS specific series (from the row itself, since we filtered date == last_date)
            series_last_date = pd.to_datetime(row["date"])
            product_id = row["product_id"]
            region_id = row["region_id"]
            
            # Check if series is dense or sparse
            is_dense = series_dense_map.get((product_id, region_id), True)  # Default to dense if not found
            
            if is_dense:
                # Use LightGBM model (recursive forecast)
                n_dense_forecast += 1
                series_forecasts = recursive_forecast_single_series(
                    model, row, feature_cols, series_last_date, horizon_days, target_transform, q_low, q_high
                )
                model_name = f"lightgbm_{target_type.value}_recursive"
            else:
                # Use baseline forecast (last 28-day average)
                n_sparse_forecast += 1
                series_forecasts = baseline_forecast_single_series(
                    history_df=df_history,
                    product_id=product_id,
                    region_id=region_id,
                    last_date=series_last_date,
                    horizon_days=horizon_days,
                    target_transform=target_transform,
                )
                model_name = f"baseline_{target_type.value}_28day_avg"
            
            # Convert to dataframe rows
            # forecast_date is calculated inside recursive_forecast_single_series using real dates
            for h, yhat, yhat_lo, yhat_hi in series_forecasts:
                # Calculate forecast_date using real date arithmetic
                forecast_date = series_last_date + timedelta(days=h)
                all_forecasts.append({
                    "forecast_date": forecast_date,
                    "product_id": product_id,
                    "region_id": region_id,
                    "horizon": h,
                    "yhat": yhat,
                    "yhat_lo": yhat_lo,
                    "yhat_hi": yhat_hi,
                    "target_type": target_type.value,  # NEW: Track target type
                    "model_name": model_name,
                    "model_version": run_id[:8],
                    "run_id": run_id,
                    "generated_at": pd.Timestamp.utcnow(),
                })
            
            # Progress indicator
            if (idx + 1) % 100 == 0:
                print(f"   Processed {idx + 1}/{len(df_pandas)} series...")
        
        # Create final forecast dataframe
        final_forecast = pd.DataFrame(all_forecasts)
        
        print(f"\n✅ Forecast generation complete:")
        print(f"   Dense series (LightGBM): {n_dense_forecast} series")
        print(f"   Sparse series (Baseline): {n_sparse_forecast} series")
        print(f"   Total forecast records: {len(final_forecast):,}")
        
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
