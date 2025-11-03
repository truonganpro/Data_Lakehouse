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
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error

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
    target_transform: str = "log1p",  # "log1p" or "none"
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
    
    # Prepare data
    df = features_df.copy()
    
    # Keep necessary columns
    feature_cols = [
        "lag_1", "lag_7", "lag_28", "roll7", "roll28",
        "dow", "month", "is_weekend", "price"
    ]
    
    # Drop rows with missing lags
    df = df.dropna(subset=["y"] + feature_cols)
    df = df.sort_values(["product_id", "region_id", "date"])
    
    # Target transformation
    if target_transform == "log1p":
        df["y_transformed"] = np.log1p(df["y"])
    else:
        df["y_transformed"] = df["y"]
    
    X = df[feature_cols]
    y = df["y_transformed"]
    
    # Time series split
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
    with mlflow.start_run(run_name=f"lgbm_global_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}") as run:
        mlflow.log_params(params)
        mlflow.log_param("target_transform", target_transform)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("n_samples", len(df))
        
        rmses, smapes = [], []
        
        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            print(f"\n📊 Fold {fold + 1}/{n_splits}")
            
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
            
            # Inverse transform
            if target_transform == "log1p":
                y_pred = np.expm1(y_pred_transformed)
                y_val_orig = np.expm1(y_val)
            else:
                y_pred = y_pred_transformed
                y_val_orig = y_val
            
            # Ensure non-negative
            y_pred = np.maximum(0, y_pred)
            
            # Metrics
            rmse = np.sqrt(mean_squared_error(y_val_orig, y_pred))
            s = smape(y_val_orig, y_pred)
            
            rmses.append(rmse)
            smapes.append(s)
            
            print(f"   RMSE: {rmse:.2f}, sMAPE: {s:.2f}%")
            
            mlflow.log_metric(f"fold_{fold}_rmse", rmse)
            mlflow.log_metric(f"fold_{fold}_smape", s)
        
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

