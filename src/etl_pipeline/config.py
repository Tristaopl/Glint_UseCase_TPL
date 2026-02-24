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

# Ensure directories exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

# File formats
FILE_FORMAT = "parquet"  # or "csv", "json", etc.

# ETL Pipeline Mode
INCREMENTAL_MODE = True # Set to True for incremental append, False for full rebuild
RUN_QUALITY_CHECKS = False  # Enable/disable quality checks

# Data quality thresholds
MIN_DATA_QUALITY_SCORE = 0.5
MAX_NULL_PERCENTAGE = 0.9

# Logging
LOG_LEVEL = "INFO"
