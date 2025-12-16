from ..job import reload_data 
from dagster import ScheduleDefinition

'''

Crontab Syntax
+---------------- minute (0 - 59)
|  +------------- hour (0 - 23)
|  |  +---------- day of month (1 - 31)
|  |  |  +------- month (1 - 12)
|  |  |  |  +---- day of week (0 - 6) (Sunday is 0 or 7)
|  |  |  |  |
*  *  *  *  *  command to be executed

* means all values are acceptable

'''

reload_data_schedule = ScheduleDefinition(
    job=reload_data,
    cron_schedule="0 0 * * *",  # every day at 00:00
)

# Export maintenance schedules
try:
    from .maintenance_schedule import (
        daily_compaction_gold_schedule,
        daily_compaction_platinum_schedule,
        weekly_vacuum_schedule,
        daily_small_files_monitor_schedule
    )
except ImportError:
    pass

# Export optimize schedule
try:
    from .optimize_schedule import daily_optimize_lakehouse_schedule
except ImportError:
    pass







# Path: etl_pipeline/etl_pipeline/schedule/__init__.py