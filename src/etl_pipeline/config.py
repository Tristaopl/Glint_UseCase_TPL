"""
Configuration settings for the ETL pipeline
"""
import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

# Layer directories
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
GOLD_STAGING_DIR = DATA_DIR / "gold_staging"

# Ensure directories exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)
GOLD_STAGING_DIR.mkdir(parents=True, exist_ok=True)

# File formats
FILE_FORMAT = "parquet"  # or "csv", "json", etc.

# ETL Pipeline Mode
INCREMENTAL_MODE = True # Set to True for incremental append, False for full rebuild

