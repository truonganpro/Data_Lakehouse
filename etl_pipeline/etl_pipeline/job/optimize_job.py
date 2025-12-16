# -*- coding: utf-8 -*-
"""
Optimize Lakehouse Job - Run OPTIMIZE & VACUUM for Delta Tables
Author: Truong An
Project: Data Lakehouse - Modern Data Stack
"""
import subprocess
import os
from dagster import job, op, OpExecutionContext


@op(
    name="run_optimize_script",
    description="Run optimize_lakehouse.sh script to optimize and vacuum Delta tables"
)
def run_optimize_script(context: OpExecutionContext):
    """
    Execute optimize_lakehouse.sh script in Spark container.
    This script runs OPTIMIZE and VACUUM on all Gold and Platinum tables.
    """
    script_path = "/scripts/optimize_lakehouse.sh"
    
    # Check if script exists
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    context.log.info(f"🚀 Starting lakehouse optimization...")
    context.log.info(f"📝 Script path: {script_path}")
    
    try:
        # Execute script in Spark master container
        result = subprocess.run(
            ["docker", "exec", "spark-master", "bash", script_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        context.log.info("✅ Optimize script completed successfully")
        context.log.info(f"Output:\n{result.stdout}")
        
        if result.stderr:
            context.log.warning(f"Warnings:\n{result.stderr}")
            
        return {"status": "success", "output": result.stdout}
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"❌ Optimize script failed: {e}")
        context.log.error(f"Error output: {e.stderr}")
        raise
    except Exception as e:
        context.log.error(f"❌ Unexpected error: {e}")
        raise


@job(
    name="optimize_lakehouse_job",
    description="Optimize and vacuum Delta Lake tables in Gold and Platinum layers"
)
def optimize_lakehouse_job():
    """
    Complete optimization job:
    1. Run OPTIMIZE on all Gold and Platinum tables (compaction)
    2. Run VACUUM on all tables (retain 168 hours = 7 days)
    
    This job should run daily after ETL pipeline completes.
    """
    run_optimize_script()

