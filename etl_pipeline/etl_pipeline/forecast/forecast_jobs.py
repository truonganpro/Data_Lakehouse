"""
Dagster jobs and schedules for Demand Forecasting
"""
from dagster import job, schedule, ScheduleDefinition
from .forecast_ops import (
    op_build_features,
    op_train_model,
    op_batch_predict,
    op_monitor_forecast,
)


@job(
    name="forecast_job",
    description="Daily demand forecasting pipeline: features → train → predict → monitor",
)
def forecast_job():
    """
    Complete forecasting pipeline:
    1. Build features from gold.factorderitem + gold.factorder
    2. Train LightGBM model
    3. Generate 28-day forecasts
    4. Monitor accuracy
    """
    features_table = op_build_features()
    run_id = op_train_model(features_table)
    forecast_table = op_batch_predict(run_id, features_table)
    op_monitor_forecast(forecast_table)


@schedule(
    cron_schedule="0 3 * * *",  # Every day at 3:00 AM
    job=forecast_job,
    execution_timezone="Asia/Ho_Chi_Minh",
)
def daily_forecast_schedule(_context):
    """Run forecast pipeline daily at 3:00 AM Vietnam time"""
    return {}

