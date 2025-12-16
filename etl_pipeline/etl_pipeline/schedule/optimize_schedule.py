# -*- coding: utf-8 -*-
"""
Optimize Lakehouse Schedule
Author: Truong An
Project: Data Lakehouse - Modern Data Stack
"""
from dagster import schedule, ScheduleEvaluationContext, DefaultScheduleStatus, RunRequest
from etl_pipeline.job.optimize_job import optimize_lakehouse_job


@schedule(
    job=optimize_lakehouse_job,
    cron_schedule="0 3 * * *",  # Every day at 3:00 AM (after ETL completes)
    default_status=DefaultScheduleStatus.RUNNING,
    name="daily_optimize_lakehouse",
    execution_timezone="Asia/Ho_Chi_Minh"
)
def daily_optimize_lakehouse_schedule(context: ScheduleEvaluationContext):
    """
    Daily schedule to optimize and vacuum Delta Lake tables.
    
    Runs at 3:00 AM daily, after ETL pipeline completes (which runs at 00:00).
    This ensures:
    - Gold and Platinum tables are compacted (OPTIMIZE)
    - Old files are cleaned up (VACUUM with 7-day retention)
    """
    return RunRequest(
        run_key=f"optimize_lakehouse_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        tags={
            "layer": "gold_platinum",
            "operation": "optimize_vacuum",
            "retention_hours": "168"
        }
    )

