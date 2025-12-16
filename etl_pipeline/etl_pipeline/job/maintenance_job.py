# -*- coding: utf-8 -*-
"""
Maintenance Jobs for Lakehouse Optimization
"""
from dagster import AssetSelection, define_asset_job

# Import maintenance assets
from etl_pipeline.assets.maintenance import (
    compact_gold_fact_order_item_recent_partitions,
    compact_platinum_datamarts,
    vacuum_gold_and_platinum,
    small_files_monitor
)

# Job: Compact recent gold partitions
compact_recent_partitions_job = define_asset_job(
    name="compact_recent_partitions_job",
    selection=AssetSelection.assets(compact_gold_fact_order_item_recent_partitions),
    description="Compact recent partitions of gold.fact_order_item"
)

# Job: Compact platinum datamarts
compact_platinum_job = define_asset_job(
    name="compact_platinum_job",
    selection=AssetSelection.assets(compact_platinum_datamarts),
    description="Compact platinum datamarts"
)

# Job: VACUUM tables
vacuum_job = define_asset_job(
    name="vacuum_job",
    selection=AssetSelection.assets(vacuum_gold_and_platinum),
    description="VACUUM gold and platinum tables"
)

# Job: Monitor small files
monitor_job = define_asset_job(
    name="monitor_job",
    selection=AssetSelection.assets(small_files_monitor),
    description="Monitor small files across tables"
)

# Combined job: All maintenance tasks
all_maintenance_job = define_asset_job(
    name="all_maintenance_job",
    selection=AssetSelection.assets(
        small_files_monitor,
        compact_gold_fact_order_item_recent_partitions,
        compact_platinum_datamarts,
        vacuum_gold_and_platinum
    ),
    description="Run all maintenance tasks"
)

