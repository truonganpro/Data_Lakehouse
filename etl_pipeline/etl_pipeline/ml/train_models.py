"""
Model Training for Demand Forecasting
Supports LightGBM (global) and Prophet (baseline aggregate)
"""
import os
import warnings
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from datetime import datetime
from enum import Enum
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error


class TargetType(str, Enum):
    """Target type for forecasting"""
    REVENUE = "revenue"
    QUANTITY = "quantity"

warnings.filterwarnings("ignore")

# Try import ML libraries
try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    print("⚠️  LightGBM not available. Install: pip install lightgbm")

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    print("⚠️  Prophet not available. Install: pip install prophet")


def smape(y_true, y_pred):
    """Symmetric Mean Absolute Percentage Error"""
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    denom = (np.abs(y_true) + np.abs(y_pred)) / 2.0
    denom = np.where(denom == 0, 1, denom)  # avoid division by zero
    diff = np.abs(y_true - y_pred) / denom
    return np.mean(diff) * 100


def load_features_from_delta(
    table_name: str = "silver.forecast_features",
    delta_root: str = "s3a://lakehouse",
    sample_frac: float = 1.0,
):
    """
    Load features from Delta table using PySpark and convert to Pandas
    
    Args:
        table_name: Delta table name
        delta_root: Root path for Delta tables
        sample_frac: Sample fraction (0-1) for faster dev iteration
    
    Returns:
        pandas DataFrame
    """
    from pyspark.sql import SparkSession
    
    jars_dir = os.getenv("JARS_DIR", "/opt/jars")
    spark_jars = ",".join([
        f"file://{jars_dir}/delta-core_2.12-2.3.0.jar",
        f"file://{jars_dir}/delta-storage-2.3.0.jar",
        f"file://{jars_dir}/hadoop-aws-3.3.2.jar",
        f"file://{jars_dir}/aws-java-sdk-bundle-1.11.1026.jar",
    ])
    
    builder = (
        SparkSession.builder
        .appName("load_features")
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
        table_path = f"{delta_root}/{table_name.replace('.', '/')}"
        df_spark = spark.read.format("delta").load(table_path)
        
        if sample_frac < 1.0:
            df_spark = df_spark.sample(fraction=sample_frac, seed=42)
        
        df = df_spark.toPandas()
        print(f"✅ Loaded {len(df):,} rows from {table_name}")
        return df
    finally:
        spark.stop()


def train_lightgbm(
    features_df: pd.DataFrame = None,
    features_table: str = "silver.forecast_features",
    mlflow_experiment: str = "demand_forecast",
    n_splits: int = 5,
    target_type: TargetType = TargetType.REVENUE,  # "revenue" or "quantity"
    target_transform: str = "log1p",  # "log1p" or "none" (usually log1p)
):
    """
    Train LightGBM global model for demand forecasting
    
    Args:
        features_df: Pre-loaded features DataFrame (if None, load from table)
        features_table: Delta table name to load features from
        mlflow_experiment: MLflow experiment name
        n_splits: Number of time series CV splits
        target_transform: "log1p" or "none"
    
    Returns:
        run_id: MLflow run ID
    """
    if not LIGHTGBM_AVAILABLE:
        raise ImportError("LightGBM not installed. Run: pip install lightgbm")
    
    # Setup MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb"))
    mlflow.set_experiment(mlflow_experiment)
    
    # Load features
    if features_df is None:
        features_df = load_features_from_delta(features_table)
    
    # Load series statistics (dense vs sparse flags)
    delta_root = os.getenv("DELTA_ROOT", "s3a://lakehouse")
    stats_table = features_table.replace("forecast_features", "forecast_series_stats")
    
    print(f"📊 Loading series statistics from {stats_table}...")
    try:
        stats_df = load_features_from_delta(stats_table)
        # Create lookup dictionary for is_dense flag
        series_dense_map = dict(
            zip(
                zip(stats_df["product_id"], stats_df["region_id"]),
                stats_df["is_dense"]
            )
        )
        
        # Add is_dense flag to features dataframe
        features_df["is_dense"] = features_df.apply(
            lambda row: series_dense_map.get((row["product_id"], row["region_id"]), False),
            axis=1
        )
        
        n_dense = features_df["is_dense"].sum()
        n_sparse = (~features_df["is_dense"]).sum()
        print(f"   ✅ Loaded series stats:")
        print(f"      Dense series: {n_dense:,} rows ({n_dense/len(features_df)*100:.1f}%)")
        print(f"      Sparse series: {n_sparse:,} rows ({n_sparse/len(features_df)*100:.1f}%)")
    except Exception as e:
        print(f"   ⚠️  Could not load series stats: {e}")
        print(f"   ⚠️  Will train on ALL series (no filtering)")
        features_df["is_dense"] = True  # Default to dense if stats not available
    
    # Prepare data - FILTER to only dense series for training
    df = features_df[features_df["is_dense"]].copy()
    
    if len(df) == 0:
        raise ValueError("No dense series found for training! Check series statistics table.")
    
    print(f"\n📚 Training on {len(df):,} samples (dense series only)")
    print(f"   Excluded {len(features_df) - len(df):,} sparse series rows")
    
    # IMPORTANT: Sort by date first to ensure chronological order for TimeSeriesSplit
    # This ensures each fold is "past → future" correctly
    df = df.sort_values(["date", "product_id", "region_id"]).reset_index(drop=True)
    
    # Select target column based on target_type
    if target_type == TargetType.REVENUE:
        target_col = "y_revenue" if "y_revenue" in df.columns else "y"  # Fallback to y for backward compatibility
        original_target_col = "revenue" if "revenue" in df.columns else None
        model_name_suffix = "revenue"
    elif target_type == TargetType.QUANTITY:
        target_col = "y_quantity"
        original_target_col = "quantity"
        model_name_suffix = "quantity"
    else:
        raise ValueError(f"Unknown target_type: {target_type}. Must be TargetType.REVENUE or TargetType.QUANTITY")
    
    print(f"\n📊 Training model for target: {target_type.value}")
    print(f"   Target column: {target_col}")
    
    # Target transformation (usually already log1p transformed in features, but allow override)
    if target_transform == "log1p":
        if target_col in df.columns:
            # Already transformed in features
            df["y_transformed"] = df[target_col]
        else:
            # Fallback: transform from original scale
            if original_target_col and original_target_col in df.columns:
                df["y_transformed"] = np.log1p(df[original_target_col])
            else:
                raise ValueError(f"Target column {target_col} not found and cannot derive from {original_target_col}")
    else:
        # No transformation
        if original_target_col and original_target_col in df.columns:
            df["y_transformed"] = df[original_target_col]
        else:
            df["y_transformed"] = df[target_col]
    
    # Store original scale for metrics calculation
    if original_target_col and original_target_col in df.columns:
        df["y_orig"] = df[original_target_col]
    elif target_type == TargetType.REVENUE and "revenue" in df.columns:
        df["y_orig"] = df["revenue"]
    elif target_type == TargetType.QUANTITY and "quantity" in df.columns:
        df["y_orig"] = df["quantity"]
    else:
        # Inverse transform if we have transformed target
        if target_transform == "log1p":
            df["y_orig"] = np.expm1(df["y_transformed"])
        else:
            df["y_orig"] = df["y_transformed"]
    
    # Calculate target encodings on TRAINING data only (to avoid data leakage)
    # These will be applied consistently in batch_predict
    # Use transformed target for encoding (consistent with model training)
    print("\n📊 Computing target encodings...")
    global_mean_y = df["y_transformed"].mean()
    
    # Payment type encoding: mean target per payment_type
    if "payment_type" in df.columns:
        pay_stats = df.groupby("payment_type")["y_transformed"].mean().to_dict()
        df["payment_type_te"] = df["payment_type"].map(pay_stats)
        # Fill missing with global mean
        df["payment_type_te"] = df["payment_type_te"].fillna(global_mean_y)
        print(f"   ✅ Payment type encoding: {len(pay_stats)} categories (target: {target_type.value})")
    else:
        pay_stats = {}
        df["payment_type_te"] = global_mean_y
        print(f"   ⚠️  payment_type column not found, using global mean")
    
    # Category encoding: mean target per product_category_name
    if "product_category_name" in df.columns:
        cat_stats = df.groupby("product_category_name")["y_transformed"].mean().to_dict()
        df["category_te"] = df["product_category_name"].map(cat_stats)
        # Fill missing with global mean
        df["category_te"] = df["category_te"].fillna(global_mean_y)
        print(f"   ✅ Category encoding: {len(cat_stats)} categories (target: {target_type.value})")
    else:
        cat_stats = {}
        df["category_te"] = global_mean_y
        print(f"   ⚠️  product_category_name column not found, using global mean")
    
    # Keep necessary columns (with new features)
    feature_cols = [
        "lag_1", "lag_7", "lag_28", "roll7", "roll28",
        "qty_roll7", "qty_roll28",  # NEW: Rolling quantity features
        "dow", "month", "is_weekend", "price",
        "payment_type_te",  # NEW: Payment type encoding
        "category_te",  # NEW: Category encoding
    ]
    
    # Drop rows with missing lags
    df = df.dropna(subset=["y"] + feature_cols)
    
    X = df[feature_cols]
    y = df["y_transformed"]
    
    # Time series split (requires data sorted by time)
    tscv = TimeSeriesSplit(n_splits=n_splits)
    
    # Model parameters
    params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": 0.05,
        "num_leaves": 128,
        "max_depth": 10,
        "min_data_in_leaf": 100,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "n_estimators": 500,
        "verbosity": -1,
    }
    
    # Training with CV
    model_name_base = f"lgbm_{model_name_suffix}_global"
    with mlflow.start_run(run_name=f"{model_name_base}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}") as run:
        mlflow.log_params(params)
        mlflow.log_param("target_type", target_type.value)
        mlflow.log_param("target_transform", target_transform)
        mlflow.log_param("target_column", target_col)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("n_samples", len(df))
        
        rmses, smapes = [], []
        all_residuals = []  # Collect residuals for CI calculation
        
        for fold, (train_idx, val_idx) in enumerate(tscv.split(X), start=1):
            # Debug: Check date ranges for train/val to ensure chronological order
            # Use .iloc for positional indexing since train_idx/val_idx are positional
            train_dates = (df.iloc[train_idx]["date"].min(), df.iloc[train_idx]["date"].max())
            val_dates = (df.iloc[val_idx]["date"].min(), df.iloc[val_idx]["date"].max())
            
            print(f"\n📊 Fold {fold}/{n_splits}")
            print(f"   Train: {train_dates[0]} → {train_dates[1]} ({len(train_idx)} samples)")
            print(f"   Val  : {val_dates[0]} → {val_dates[1]} ({len(val_idx)} samples)")
            
            # Ensure chronological order: validation dates should be after training dates
            if val_dates[0] < train_dates[1]:
                print(f"   ⚠️  WARNING: Validation period overlaps with training period!")
            
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
            
            # Train
            model = lgb.LGBMRegressor(**params)
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                callbacks=[lgb.early_stopping(50, verbose=False)]
            )
            
            # Predict
            y_pred_transformed = model.predict(X_val)
            
            # Inverse transform to original scale (revenue)
            if target_transform == "log1p":
                y_pred_orig = np.expm1(y_pred_transformed)
                y_val_orig = np.expm1(y_val.values)
            else:
                y_pred_orig = y_pred_transformed
                y_val_orig = y_val.values
            
            # Ensure non-negative
            y_pred_orig = np.maximum(0, y_pred_orig)
            
            # Calculate residuals on original scale
            residuals_fold = y_val_orig - y_pred_orig
            all_residuals.extend(residuals_fold.tolist())
            
            # Metrics
            rmse = np.sqrt(mean_squared_error(y_val_orig, y_pred_orig))
            s = smape(y_val_orig, y_pred_orig)
            
            rmses.append(rmse)
            smapes.append(s)
            
            print(f"   RMSE: {rmse:.2f}, sMAPE: {s:.2f}%")
            
            mlflow.log_metric(f"fold_{fold}_rmse", rmse)
            mlflow.log_metric(f"fold_{fold}_smape", s)
            
            # Log date ranges for this fold
            mlflow.log_param(f"fold_{fold}_train_date_min", str(train_dates[0]))
            mlflow.log_param(f"fold_{fold}_train_date_max", str(train_dates[1]))
            mlflow.log_param(f"fold_{fold}_val_date_min", str(val_dates[0]))
            mlflow.log_param(f"fold_{fold}_val_date_max", str(val_dates[1]))
        
        # Calculate residual quantiles for Confidence Intervals
        all_residuals = np.array(all_residuals)
        q_low = float(np.quantile(all_residuals, 0.10))   # 10th percentile (typically negative)
        q_high = float(np.quantile(all_residuals, 0.90))  # 90th percentile (typically positive)
        
        print(f"\n📊 Residual Statistics for CI:")
        print(f"   Total residuals: {len(all_residuals):,}")
        print(f"   Residual q10 (lower): {q_low:.2f}")
        print(f"   Residual q90 (upper): {q_high:.2f}")
        print(f"   Residual mean: {np.mean(all_residuals):.2f}")
        print(f"   Residual std: {np.std(all_residuals):.2f}")
        
        # Log quantiles to MLflow for use in batch_predict
        mlflow.log_param("ci_residual_q10", q_low)
        mlflow.log_param("ci_residual_q90", q_high)
        mlflow.log_metric("ci_residual_mean", float(np.mean(all_residuals)))
        mlflow.log_metric("ci_residual_std", float(np.std(all_residuals)))
        
        # Log target encodings to MLflow for use in batch_predict
        import json
        if pay_stats:
            mlflow.log_dict(pay_stats, "payment_type_te.json")
            print(f"   ✅ Logged payment_type_te encoding ({len(pay_stats)} categories)")
        if cat_stats:
            mlflow.log_dict(cat_stats, "category_te.json")
            print(f"   ✅ Logged category_te encoding ({len(cat_stats)} categories)")
        
        # Log global mean for fallback
        global_mean_y = df["y_transformed"].mean()
        mlflow.log_param("encoding_global_mean", float(global_mean_y))
        
        # Log average metrics
        avg_rmse = float(np.mean(rmses))
        avg_smape = float(np.mean(smapes))
        
        mlflow.log_metric("cv_rmse_mean", avg_rmse)
        mlflow.log_metric("cv_smape_mean", avg_smape)
        
        print(f"\n✅ CV Results: RMSE={avg_rmse:.2f}, sMAPE={avg_smape:.2f}%")
        
        # Train final model on all data
        print("\n🔄 Training final model on all data...")
        final_model = lgb.LGBMRegressor(**params)
        final_model.fit(X, y)
        
        # Log model
        mlflow.sklearn.log_model(final_model, artifact_path="model")
        
        # Log feature importance
        importance = pd.DataFrame({
            "feature": feature_cols,
            "importance": final_model.feature_importances_
        }).sort_values("importance", ascending=False)
        
        mlflow.log_text(importance.to_string(), "feature_importance.txt")
        
        run_id = run.info.run_id
        print(f"✅ Model logged to MLflow: run_id={run_id}")
        
        return run_id


def train_prophet_aggregate(
    features_df: pd.DataFrame = None,
    features_table: str = "silver.forecast_features",
    horizon_days: int = 28,
    mlflow_experiment: str = "demand_forecast",
):
    """
    Train Prophet model on aggregated data (baseline)
    
    Args:
        features_df: Pre-loaded features DataFrame
        features_table: Delta table name
        horizon_days: Forecast horizon
        mlflow_experiment: MLflow experiment name
    
    Returns:
        run_id: MLflow run ID
    """
    if not PROPHET_AVAILABLE:
        raise ImportError("Prophet not installed. Run: pip install prophet")
    
    # Setup MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb"))
    mlflow.set_experiment(mlflow_experiment)
    
    # Load features
    if features_df is None:
        features_df = load_features_from_delta(features_table)
    
    # Aggregate to daily total
    agg = (
        features_df
        .groupby("date", as_index=False)["y"]
        .sum()
        .rename(columns={"date": "ds", "y": "y"})
    )
    
    with mlflow.start_run(run_name=f"prophet_baseline_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}") as run:
        # Train Prophet
        model = Prophet(
            weekly_seasonality=True,
            yearly_seasonality=True,
            daily_seasonality=False,
        )
        model.fit(agg)
        
        # Make forecast
        future = model.make_future_dataframe(periods=horizon_days)
        forecast = model.predict(future)
        
        # Log parameters
        mlflow.log_param("model_type", "prophet_aggregate")
        mlflow.log_param("horizon_days", horizon_days)
        mlflow.log_param("n_train_days", len(agg))
        
        # Log model
        mlflow.sklearn.log_model(model, artifact_path="model")
        
        run_id = run.info.run_id
        print(f"✅ Prophet model logged: run_id={run_id}")
        
        return run_id


if __name__ == "__main__":
    # For standalone testing
    print("🚀 Training LightGBM model...")
    run_id = train_lightgbm()
    print(f"\n✅ Completed! Run ID: {run_id}")

