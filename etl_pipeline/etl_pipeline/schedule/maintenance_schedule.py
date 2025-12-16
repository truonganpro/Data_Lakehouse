# -*- coding: utf-8 -*-
"""
Maintenance Schedules for Lakehouse Optimization
"""
from dagster import schedule, ScheduleEvaluationContext
from dagster import DefaultScheduleStatus, RunRequest
from etl_pipeline.job.maintenance_job import (
    compact_recent_partitions_job,
    compact_platinum_job,
    vacuum_job,
    monitor_job
)


@schedule(
    job=compact_recent_partitions_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    default_status=DefaultScheduleStatus.RUNNING,
    name="daily_compaction_gold"
)
def daily_compaction_gold_schedule(context: ScheduleEvaluationContext):
    """Daily compaction of recent gold table partitions"""
    return RunRequest(run_key=f"compact_gold_{context.scheduled_execution_time.strftime('%Y%m%d')}")


@schedule(
    job=compact_platinum_job,
    cron_schedule="0 3 * * *",  # Daily at 3 AM
    default_status=DefaultScheduleStatus.RUNNING,
    name="daily_compaction_platinum"
)
def daily_compaction_platinum_schedule(context: ScheduleEvaluationContext):
    """Daily compaction of platinum datamarts"""
    return RunRequest(run_key=f"compact_platinum_{context.scheduled_execution_time.strftime('%Y%m%d')}")


@schedule(
    job=vacuum_job,
    cron_schedule="0 4 * * 0",  # Weekly on Sunday at 4 AM
    default_status=DefaultScheduleStatus.RUNNING,
    name="weekly_vacuum"
)
def weekly_vacuum_schedule(context: ScheduleEvaluationContext):
    """Weekly VACUUM of gold and platinum tables"""
    return RunRequest(run_key=f"vacuum_{context.scheduled_execution_time.strftime('%Y%m%d')}")


@schedule(
    job=monitor_job,
    cron_schedule="0 1 * * *",  # Daily at 1 AM (before compaction)
    default_status=DefaultScheduleStatus.RUNNING,
    name="daily_small_files_monitor"
)
def daily_small_files_monitor_schedule(context: ScheduleEvaluationContext):
    """Daily monitoring of small files"""
    return RunRequest(run_key=f"monitor_{context.scheduled_execution_time.strftime('%Y%m%d')}")

