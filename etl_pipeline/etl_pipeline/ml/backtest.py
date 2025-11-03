"""
Backtesting for Demand Forecasting
Performs rolling-origin validation
"""
import os
import numpy as np
import pandas as pd
import mlflow
from datetime import datetime, timedelta
from typing import List, Tuple


def smape(y_true, y_pred):
    """Symmetric Mean Absolute Percentage Error"""
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    denom = (np.abs(y_true) + np.abs(y_pred)) / 2.0
    denom = np.where(denom == 0, 1, denom)
    diff = np.abs(y_true - y_pred) / denom
    return np.mean(diff) * 100


def mase(y_true, y_pred, y_train):
    """Mean Absolute Scaled Error"""
    mae = np.mean(np.abs(y_true - y_pred))
    mae_naive = np.mean(np.abs(np.diff(y_train)))
    return mae / mae_naive if mae_naive != 0 else np.inf


def rolling_backtest(
    features_df: pd.DataFrame,
    train_func,
    predict_func,
    horizons: List[int] = [7, 14, 28],
    n_splits: int = 4,
    min_train_days: int = 90,
    mlflow_experiment: str = "demand_forecast_backtest",
):
    """
    Perform rolling-origin backtesting
    
    Args:
        features_df: Features DataFrame with columns [date, product_id, region_id, y, ...]
        train_func: Training function(train_df) -> model
        predict_func: Prediction function(model, features, horizon) -> predictions
        horizons: List of forecast horizons to evaluate
        n_splits: Number of time splits
        min_train_days: Minimum training window size
        mlflow_experiment: MLflow experiment name
    
    Returns:
        results_df: DataFrame with backtest results
    """
    
    # Setup MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "mysql+pymysql://mlflow:mlflow@de_mysql:3306/mlflowdb"))
    mlflow.set_experiment(mlflow_experiment)
    
    # Sort by date
    df = features_df.sort_values("date")
    
    # Determine split dates
    date_range = (df["date"].max() - df["date"].min()).days
    test_window = date_range // (n_splits + 1)
    
    split_dates = []
    for i in range(n_splits):
        cutoff = df["date"].min() + timedelta(days=min_train_days + i * test_window)
        split_dates.append(cutoff)
    
    print(f"📊 Backtesting with {n_splits} splits, horizons: {horizons}")
    print(f"   Split dates: {[d.strftime('%Y-%m-%d') for d in split_dates]}")
    
    results = []
    
    with mlflow.start_run(run_name=f"backtest_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_param("n_splits", n_splits)
        mlflow.log_param("horizons", str(horizons))
        mlflow.log_param("min_train_days", min_train_days)
        
        for fold, cutoff_date in enumerate(split_dates):
            print(f"\n🔄 Fold {fold + 1}/{n_splits} - cutoff: {cutoff_date.strftime('%Y-%m-%d')}")
            
            # Split train/test
            train_df = df[df["date"] <= cutoff_date]
            
            if len(train_df) < min_train_days:
                print(f"   ⚠️  Skipping - insufficient training data")
                continue
            
            # Train model
            print(f"   📚 Training on {len(train_df)} samples...")
            model = train_func(train_df)
            
            # Evaluate on each horizon
            for horizon in horizons:
                test_start = cutoff_date + timedelta(days=1)
                test_end = cutoff_date + timedelta(days=horizon)
                
                test_df = df[(df["date"] >= test_start) & (df["date"] <= test_end)]
                
                if len(test_df) == 0:
                    continue
                
                # Predict
                predictions = predict_func(model, train_df.tail(1), horizon)
                
                # Calculate metrics
                y_true = test_df["y"].values
                y_pred = predictions[:len(y_true)]  # match length
                
                if len(y_pred) != len(y_true):
                    print(f"   ⚠️  Length mismatch: pred={len(y_pred)}, true={len(y_true)}")
                    continue
                
                smape_score = smape(y_true, y_pred)
                rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
                mae = np.mean(np.abs(y_true - y_pred))
                
                result = {
                    "fold": fold,
                    "cutoff_date": cutoff_date,
                    "horizon": horizon,
                    "smape": smape_score,
                    "rmse": rmse,
                    "mae": mae,
                    "n_train": len(train_df),
                    "n_test": len(test_df),
                }
                
                results.append(result)
                
                print(f"   H{horizon:2d}: sMAPE={smape_score:5.2f}%, RMSE={rmse:7.2f}, MAE={mae:7.2f}")
                
                # Log to MLflow
                mlflow.log_metric(f"fold_{fold}_h{horizon}_smape", smape_score)
                mlflow.log_metric(f"fold_{fold}_h{horizon}_rmse", rmse)
        
        # Aggregate results
        results_df = pd.DataFrame(results)
        
        if len(results_df) > 0:
            summary = results_df.groupby("horizon").agg({
                "smape": ["mean", "std"],
                "rmse": ["mean", "std"],
                "mae": ["mean", "std"],
            }).round(2)
            
            print("\n📈 Backtest Summary:")
            print(summary)
            
            # Log summary metrics
            for horizon in horizons:
                horizon_data = results_df[results_df["horizon"] == horizon]
                if len(horizon_data) > 0:
                    mlflow.log_metric(f"h{horizon}_smape_mean", horizon_data["smape"].mean())
                    mlflow.log_metric(f"h{horizon}_rmse_mean", horizon_data["rmse"].mean())
            
            # Save results
            mlflow.log_text(results_df.to_csv(index=False), "backtest_results.csv")
        else:
            print("⚠️  No backtest results generated")
        
        return results_df


if __name__ == "__main__":
    print("💡 Backtest module - import and use rolling_backtest() function")

