"""
Airflow Assets shared between DAGs.
"""

from airflow.sdk import Asset

# Emitted by taxi_monthly_ingest.load_raw_postgres on success; schedules
# taxi_transform_and_test.
RAW_TRIPS_LOADED = Asset("raw_trips_loaded")
