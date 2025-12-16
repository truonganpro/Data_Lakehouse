"""
Backtesting for Demand Forecasting
Performs rolling-origin validation with baseline comparison
"""
import os
import numpy as np
import pandas as pd
import mlflow
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from etl_pipeline.ml.batch_predict import TargetType

try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    print("⚠️  LightGBM not available. Install: pip install lightgbm")


def smape(y_true, y_pred, eps=1e-8):
    """Symmetric Mean Absolute Percentage Error"""
    y_true = np.array(y_true, dtype=float)
    y_pred = np.array(y_pred, dtype=float)
    denom = (np.abs(y_true) + np.abs(y_pred)) + eps
    return np.mean(2.0 * np.abs(y_pred - y_true) / denom) * 100.0


def mae(y_true, y_pred):
    """Mean Absolute Error"""
    y_true = np.array(y_true, dtype=float)
    y_pred = np.array(y_pred, dtype=float)
    return np.mean(np.abs(y_pred - y_true))


def recursive_forecast_for_all_series(
    model, df_history, cutoff_date, max_horizon, feature_cols, target_transform="log1p", q_low=None, q_high=None
):
    """
    Generate recursive forecasts for all series using history up to cutoff_date
    
    Args:
        model: Trained LightGBM model
        df_history: DataFrame with historical features (date <= cutoff_date)
        cutoff_date: Last date with real data
        max_horizon: Maximum horizon to forecast
        feature_cols: List of feature column names
        target_transform: "log1p" or "none"
        q_low: Lower residual quantile for CI
        q_high: Upper residual quantile for CI
    
    Returns:
        DataFrame with columns: [product_id, region_id, forecast_date, horizon, yhat]
    """
    from etl_pipeline.ml.batch_predict import recursive_forecast_single_series
    
    # Get most recent features per series (at cutoff_date)
    df_latest = (
        df_history[df_history["date"] <= cutoff_date]
        .sort_values("date")
        .groupby(["product_id", "region_id"])
        .last()
        .reset_index()
    )
    
    all_forecasts = []
    
    for idx, row in df_latest.iterrows():
        series_last_date = pd.to_datetime(row["date"])
        
        # Get recursive forecasts for this series
        series_forecasts = recursive_forecast_single_series(
            model, row, feature_cols, series_last_date, max_horizon, target_transform, q_low, q_high
        )
        
        # Convert to dataframe rows
        for h, yhat, yhat_lo, yhat_hi in series_forecasts:
            forecast_date = series_last_date + timedelta(days=h)
            all_forecasts.append({
                "product_id": row["product_id"],
                "region_id": row["region_id"],
                "forecast_date": forecast_date,
                "horizon": h,
                "yhat": yhat,
            })
    
    return pd.DataFrame(all_forecasts)


def rolling_backtest(
    features_df: pd.DataFrame,
    cutoff_dates: List[pd.Timestamp],
    horizons: List[int] = [7, 14, 28],
    feature_cols: Optional[List[str]] = None,
    target_transform: str = "log1p",
    target_type: str = "revenue",  # "revenue" or "quantity"
    model_params: Optional[dict] = None,
    q_low: Optional[float] = None,
    q_high: Optional[float] = None,
    mlflow_experiment: str = "demand_forecast_backtest",
    run_id: Optional[str] = None,
):
    """
    Perform rolling-origin backtesting with baseline comparison
    
    Args:
        features_df: Features DataFrame with columns [date, product_id, region_id, y, ...]
        cutoff_dates: List of cutoff dates for backtesting
        horizons: List of forecast horizons to evaluate [7, 14, 28]
        feature_cols: List of feature column names (if None, auto-detect)
        target_transform: "log1p" or "none"
        model_params: LightGBM parameters (if None, use defaults)
        q_low: Lower residual quantile for CI
        q_high: Upper residual quantile for CI
        mlflow_experiment: MLflow experiment name
        run_id: Optional run_id to link backtest to training run
    
    Returns:
        Tuple of (df_backtest_detail, df_metrics)
    """
    if not LIGHTGBM_AVAILABLE:
        raise ImportError("LightGBM not installed. Run: pip install lightgbm")
    
    # Setup MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb"))
    mlflow.set_experiment(mlflow_experiment)
    
    # Sort by date
    df = features_df.copy()
    df = df.sort_values(["date", "product_id", "region_id"]).reset_index(drop=True)
    
    # Auto-detect feature columns if not provided
    if feature_cols is None:
        feature_cols = [
            "lag_1", "lag_7", "lag_28", "roll7", "roll28",
            "qty_roll7", "qty_roll28",  # NEW: Rolling quantity features
            "dow", "month", "is_weekend", "price",
            "payment_type_te",  # NEW: Payment type encoding (should be computed from training data)
            "category_te",  # NEW: Category encoding (should be computed from training data)
        ]
    
    # Default model parameters
    if model_params is None:
        model_params = {
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
    
    max_h = max(horizons)
    
    print(f"📊 Rolling Backtest")
    print(f"   Cutoff dates: {[d.strftime('%Y-%m-%d') for d in cutoff_dates]}")
    print(f"   Horizons: {horizons}")
    print(f"   Max horizon: {max_h}")
    
    # Prepare target transformation
    # Note: features_df should have "y" in original scale (revenue)
    # We'll transform it for training, but keep original for metrics
    if target_transform == "log1p":
        df["y_orig"] = df["y"]  # Assume y in features_df is original scale
        df["y_transformed"] = np.log1p(df["y_orig"])
    else:
        df["y_orig"] = df["y"]
        df["y_transformed"] = df["y"]
    
    # Drop rows with missing features
    df = df.dropna(subset=feature_cols + ["y_orig", "y_transformed"])
    
    results = []  # Will contain all forecast vs actual for all cutoff/horizon
    
    with mlflow.start_run(run_name=f"backtest_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_param("cutoff_dates", str([d.strftime('%Y-%m-%d') for d in cutoff_dates]))
        mlflow.log_param("horizons", str(horizons))
        mlflow.log_param("target_transform", target_transform)
        if run_id:
            mlflow.log_param("training_run_id", run_id)
        
        for cutoff_idx, cutoff_date in enumerate(cutoff_dates):
            print(f"\n🔄 Backtest at cutoff: {cutoff_date.strftime('%Y-%m-%d')} ({cutoff_idx + 1}/{len(cutoff_dates)})")
            
            # 1) Split train/test by time
            df_train = df[df["date"] <= cutoff_date].copy()
            df_test = df[
                (df["date"] > cutoff_date) & 
                (df["date"] <= cutoff_date + pd.Timedelta(days=max_h))
            ].copy()
            
            if df_test.empty:
                print(f"   ⚠️  Skipping - no test data")
                continue
            
            if len(df_train) < 100:  # Minimum training samples
                print(f"   ⚠️  Skipping - insufficient training data ({len(df_train)} samples)")
                continue
            
            # 2) Compute target encodings on TRAINING data only (for this cutoff)
            # This mimics the training process where encodings are computed on train set
            global_mean_y = df_train["y_transformed"].mean()
            
            # Payment type encoding
            if "payment_type" in df_train.columns:
                pay_stats = df_train.groupby("payment_type")["y_transformed"].mean().to_dict()
                df_train["payment_type_te"] = df_train["payment_type"].map(pay_stats).fillna(global_mean_y)
                df_test["payment_type_te"] = df_test["payment_type"].map(pay_stats).fillna(global_mean_y)
            else:
                df_train["payment_type_te"] = global_mean_y
                df_test["payment_type_te"] = global_mean_y
            
            # Category encoding
            if "product_category_name" in df_train.columns:
                cat_stats = df_train.groupby("product_category_name")["y_transformed"].mean().to_dict()
                df_train["category_te"] = df_train["product_category_name"].map(cat_stats).fillna(global_mean_y)
                df_test["category_te"] = df_test["product_category_name"].map(cat_stats).fillna(global_mean_y)
            else:
                df_train["category_te"] = global_mean_y
                df_test["category_te"] = global_mean_y
            
            # Prepare train matrix
            X_train = df_train[feature_cols]
            y_train = df_train["y_transformed"]
            
            # 3) Train model
            print(f"   📚 Training on {len(df_train):,} samples...")
            model = lgb.LGBMRegressor(**model_params)
            model.fit(X_train, y_train)
            
            # 4) Forecast for all series
            print(f"   🔮 Forecasting for {df_train.groupby(['product_id', 'region_id']).ngroups} series...")
            forecasts = recursive_forecast_for_all_series(
                model=model,
                df_history=df_train,
                cutoff_date=cutoff_date,
                max_horizon=max_h,
                feature_cols=feature_cols,
                target_transform=target_transform,
                q_low=q_low,
                q_high=q_high,
            )
            
            # 5) Join with actual
            merged = df_test.merge(
                forecasts,
                left_on=["product_id", "region_id", "date"],
                right_on=["product_id", "region_id", "forecast_date"],
                how="inner",
            )
            
            if merged.empty:
                print(f"   ⚠️  No matching forecasts found")
                continue
            
            # 6) Create naive baseline (last value at cutoff_date)
            last_vals = (
                df_train[df_train["date"] == cutoff_date]
                .groupby(["product_id", "region_id"])["y_orig"]
                .last()
                .rename("y_last")
                .reset_index()
            )
            
            # If some series don't have data exactly at cutoff, use most recent value
            if len(last_vals) < df_train.groupby(["product_id", "region_id"]).ngroups:
                last_vals_all = (
                    df_train[df_train["date"] <= cutoff_date]
                    .sort_values("date")
                    .groupby(["product_id", "region_id"])["y_orig"]
                    .last()
                    .rename("y_last")
                    .reset_index()
                )
                last_vals = last_vals_all
            
            merged = merged.merge(
                last_vals,
                on=["product_id", "region_id"],
                how="left",
            )
            
            # Fill missing with actual (fallback)
            merged["yhat_naive"] = merged["y_last"].fillna(merged["y_orig"])
            
            # 7) Save results for this cutoff
            merged["cutoff_date"] = cutoff_date
            results.append(merged[[
                "cutoff_date", "product_id", "region_id",
                "date", "horizon", "y_orig", "yhat", "yhat_naive"
            ]])
            
            print(f"   ✅ Forecast {len(merged):,} points, {merged['horizon'].nunique()} horizons")
        
        # Concatenate all results
        if not results:
            print("⚠️  No backtest results generated")
            return pd.DataFrame(), pd.DataFrame()
        
        df_bt = pd.concat(results, ignore_index=True)
        df_bt = df_bt.rename(columns={"y_orig": "y"})
        
        print(f"\n📊 Backtest Results Summary:")
        print(f"   Total forecast points: {len(df_bt):,}")
        print(f"   Unique series: {df_bt.groupby(['product_id', 'region_id']).ngroups}")
        print(f"   Cutoff dates: {df_bt['cutoff_date'].nunique()}")
        
        # 8) Calculate metrics by horizon (model vs baseline)
        metrics_rows = []
        
        for h in horizons:
            sub = df_bt[df_bt["horizon"] == h]
            
            if len(sub) == 0:
                continue
            
            y_true = sub["y"].values
            y_pred_model = sub["yhat"].values
            y_pred_naive = sub["yhat_naive"].values
            
            # Model metrics
            metrics_rows.append({
                "horizon": h,
                "model_type": "lightgbm",
                "smape": smape(y_true, y_pred_model),
                "mae": mae(y_true, y_pred_model),
                "n_samples": len(sub),
            })
            
            # Baseline metrics
            metrics_rows.append({
                "horizon": h,
                "model_type": "naive_last",
                "smape": smape(y_true, y_pred_naive),
                "mae": mae(y_true, y_pred_naive),
                "n_samples": len(sub),
            })
        
        df_metrics = pd.DataFrame(metrics_rows)
        
        # Calculate CI coverage for model (if CI bounds available)
        ci_coverage_rows = []
        if "yhat_lo" in df_bt.columns and "yhat_hi" in df_bt.columns:
            # Only for lightgbm (baseline doesn't have CI from model)
            for h in horizons:
                sub = df_bt[(df_bt["horizon"] == h) & (df_bt["yhat_lo"].notna()) & (df_bt["yhat_hi"].notna())]
                if len(sub) > 0:
                    inside_ci = ((sub["y"] >= sub["yhat_lo"]) & (sub["y"] <= sub["yhat_hi"])).sum()
                    coverage = inside_ci / len(sub) if len(sub) > 0 else 0.0
                    ci_coverage_rows.append({
                        "horizon": h,
                        "model_type": "lightgbm",
                        "metric": "ci_coverage",
                        "value": coverage,
                    })
        
        if len(df_metrics) > 0:
            print("\n📈 Metrics by Horizon:")
            print(df_metrics.to_string(index=False))
            
            if ci_coverage_rows:
                print("\n📊 CI Coverage by Horizon:")
                for row in ci_coverage_rows:
                    print(f"   H{int(row['horizon'])}: {row['value']*100:.1f}%")
            
            # Log metrics to MLflow
            for _, row in df_metrics.iterrows():
                mlflow.log_metric(
                    f"h{int(row['horizon'])}_{row['model_type']}_smape",
                    row["smape"]
                )
                mlflow.log_metric(
                    f"h{int(row['horizon'])}_{row['model_type']}_mae",
                    row["mae"]
                )
            
            # Log CI coverage
            for row in ci_coverage_rows:
                mlflow.log_metric(
                    f"h{int(row['horizon'])}_{row['model_type']}_ci_coverage",
                    row["value"]
                )
            
            # Save results
            mlflow.log_text(df_metrics.to_csv(index=False), "backtest_metrics.csv")
            mlflow.log_text(df_bt.head(1000).to_csv(index=False), "backtest_detail_sample.csv")
            
            # Save metrics to Delta table for dashboard access
            if save_to_delta:
                try:
                    from pyspark.sql import SparkSession
                    from pyspark.sql import functions as F
                    import os
                    
                    delta_root = delta_root or os.getenv("DELTA_ROOT", "s3a://lakehouse")
                    monitoring_table = "platinum.forecast_monitoring"
                    detail_table = "platinum.forecast_backtest_detail"
                    
                    # Prepare metrics for Delta table (normalized format)
                    monitoring_rows = []
                    backtest_run_id = mlflow.active_run().info.run_id if mlflow.active_run() else None
                    created_at = datetime.utcnow()
                    
                    # Add smape and mae metrics
                    for _, row in df_metrics.iterrows():
                        # sMAPE metric
                        monitoring_rows.append({
                            "run_id": backtest_run_id or "unknown",
                            "model_name": f"lightgbm_{target_type}",
                            "model_type": row["model_type"],
                            "target_type": target_type,
                            "horizon": int(row["horizon"]),
                            "metric": "smape",
                            "value": float(row["smape"]),
                            "n_samples": int(row["n_samples"]),
                            "created_at": created_at,
                        })
                        # MAE metric
                        monitoring_rows.append({
                            "run_id": backtest_run_id or "unknown",
                            "model_name": f"lightgbm_{target_type}",
                            "model_type": row["model_type"],
                            "target_type": target_type,
                            "horizon": int(row["horizon"]),
                            "metric": "mae",
                            "value": float(row["mae"]),
                            "n_samples": int(row["n_samples"]),
                            "created_at": created_at,
                        })
                    
                    # Add CI coverage metrics
                    for row in ci_coverage_rows:
                        monitoring_rows.append({
                            "run_id": backtest_run_id or "unknown",
                            "model_name": f"lightgbm_{target_type}",
                            "model_type": row["model_type"],
                            "target_type": target_type,
                            "horizon": int(row["horizon"]),
                            "metric": "ci_coverage",
                            "value": float(row["value"]),
                            "n_samples": None,
                            "created_at": created_at,
                        })
                    
                    if monitoring_rows:
                        df_monitoring = pd.DataFrame(monitoring_rows)
                        
                        # Write to Delta using Spark
                        jars_dir = os.getenv("JARS_DIR", "/opt/jars")
                        spark_jars = ",".join([
                            f"file://{jars_dir}/delta-core_2.12-2.3.0.jar",
                            f"file://{jars_dir}/delta-storage-2.3.0.jar",
                            f"file://{jars_dir}/hadoop-aws-3.3.2.jar",
                            f"file://{jars_dir}/aws-java-sdk-bundle-1.11.1026.jar",
                        ])
                        
                        builder = (
                            SparkSession.builder
                            .appName("save_monitoring_metrics")
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
                            # Save monitoring metrics
                            sdf_metrics = spark.createDataFrame(df_monitoring)
                            metrics_path = f"{delta_root}/{monitoring_table.replace('.', '/')}"
                            sdf_metrics.write.format("delta").mode("append").option("mergeSchema", "true").save(metrics_path)
                            print(f"\n✅ Saved monitoring metrics to {monitoring_table} ({len(monitoring_rows)} rows)")
                            
                            # Save backtest detail (for series-level analysis)
                            # Add metadata columns
                            df_bt_detail = df_bt.copy()
                            df_bt_detail["run_id"] = backtest_run_id or "unknown"
                            df_bt_detail["model_name"] = f"lightgbm_{target_type}"
                            df_bt_detail["target_type"] = target_type
                            df_bt_detail["created_at"] = created_at
                            
                            # Select relevant columns
                            detail_cols = [
                                "run_id", "model_name", "target_type", "cutoff_date",
                                "product_id", "region_id", "date", "horizon",
                                "y", "yhat", "yhat_naive"
                            ]
                            if "yhat_lo" in df_bt_detail.columns and "yhat_hi" in df_bt_detail.columns:
                                detail_cols.extend(["yhat_lo", "yhat_hi"])
                            detail_cols.append("created_at")
                            
                            df_bt_detail = df_bt_detail[[c for c in detail_cols if c in df_bt_detail.columns]]
                            
                            sdf_detail = spark.createDataFrame(df_bt_detail)
                            detail_path = f"{delta_root}/{detail_table.replace('.', '/')}"
                            sdf_detail.write.format("delta").mode("append").option("mergeSchema", "true").save(detail_path)
                            print(f"✅ Saved backtest detail to {detail_table} ({len(df_bt_detail):,} rows)")
                        finally:
                            spark.stop()
                except Exception as e:
                    print(f"⚠️  Could not save monitoring data to Delta: {e}")
                    import traceback
                    traceback.print_exc()
        
        return df_bt, df_metrics


if __name__ == "__main__":
    print("💡 Backtest module - import and use rolling_backtest() function")

