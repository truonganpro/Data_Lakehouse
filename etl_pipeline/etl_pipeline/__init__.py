import os
from dagster import Definitions, load_assets_from_modules

try:
    from dagstermill import ConfigurableLocalOutputNotebookIOManager
    NOTEBOOK_IO_MANAGER_AVAILABLE = True
except ImportError:
    ConfigurableLocalOutputNotebookIOManager = None
    NOTEBOOK_IO_MANAGER_AVAILABLE = False

from .assets import bronze, silver, gold, platinum
from .assets import maintenance
from .job import reload_data, full_pipeline_job
from .job import maintenance_job
from .schedule import reload_data_schedule
from .schedule import maintenance_schedule
from .resources.minio_io_manager import MinioIOManager
from .resources.mysql_io_manager import MysqlIOManager
from .resources.spark_io_manager import SparkIOManager

# Import forecast jobs (optional - enable when ready)
try:
    from .forecast import forecast_job, daily_forecast_schedule
    FORECAST_AVAILABLE = True
except ImportError:
    FORECAST_AVAILABLE = False
    forecast_job = None
    daily_forecast_schedule = None


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "de_mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DATABASE", "brazillian_ecommerce"),
    "user": os.getenv("MYSQL_USER", "hive"),
    "password": os.getenv("MYSQL_PASSWORD", "hive"),
}


MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
    "bucket": os.getenv("DATALAKE_BUCKET", "warehouse"),
}

SPARK_CONFIG = {
    "spark_master": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
}

resources = {
    "mysql_io_manager": MysqlIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinioIOManager(MINIO_CONFIG),
    "spark_io_manager": SparkIOManager(SPARK_CONFIG),
}

if NOTEBOOK_IO_MANAGER_AVAILABLE:
    resources["output_notebook_io_manager"] = ConfigurableLocalOutputNotebookIOManager()

# Load assets from each layer separately
bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])
gold_layer_assets = load_assets_from_modules([gold])
platinum_layer_assets = load_assets_from_modules([platinum])
maintenance_assets = load_assets_from_modules([maintenance])

# Collect jobs and schedules
all_jobs = [reload_data, full_pipeline_job]

# Add maintenance jobs
try:
    from .job.maintenance_job import (
        compact_recent_partitions_job,
        compact_platinum_job,
        vacuum_job,
        monitor_job,
        all_maintenance_job
    )
    all_jobs.extend([
        compact_recent_partitions_job,
        compact_platinum_job,
        vacuum_job,
        monitor_job,
        all_maintenance_job
    ])
except ImportError:
    pass

# Add optimize job
try:
    from .job.optimize_job import optimize_lakehouse_job
    all_jobs.append(optimize_lakehouse_job)
except ImportError:
    pass

all_schedules = [reload_data_schedule]

# Add maintenance schedules
try:
    from .schedule.maintenance_schedule import (
        daily_compaction_gold_schedule,
        daily_compaction_platinum_schedule,
        weekly_vacuum_schedule,
        daily_small_files_monitor_schedule
    )
    all_schedules.extend([
        daily_compaction_gold_schedule,
        daily_compaction_platinum_schedule,
        weekly_vacuum_schedule,
        daily_small_files_monitor_schedule
    ])
except ImportError:
    pass

# Add optimize schedule
try:
    from .schedule.optimize_schedule import daily_optimize_lakehouse_schedule
    all_schedules.append(daily_optimize_lakehouse_schedule)
except ImportError:
    pass

# Add forecast job if available
if FORECAST_AVAILABLE:
    all_jobs.append(forecast_job)
    all_schedules.append(daily_forecast_schedule)

defs = Definitions(
    assets=bronze_layer_assets
    + silver_layer_assets
    + gold_layer_assets
    + platinum_layer_assets
    + maintenance_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources=resources,
)
