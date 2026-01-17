from dagster import AssetSelection, define_asset_job

# Layer constants
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
PLATINUM = "platinum"

# Asset selections for each layer
bronze_selection = AssetSelection.groups(BRONZE)
silver_selection = AssetSelection.groups(SILVER)
gold_selection = AssetSelection.groups(GOLD)
platinum_selection = AssetSelection.groups(PLATINUM)

# Individual layer jobs
reload_data = define_asset_job(
    name="reload_data",
    selection=bronze_selection,  # Default to bronze layer only
)

# Full pipeline job
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),  # Run all layers
)

# Maintenance jobs removed - using optimize_lakehouse_job instead

