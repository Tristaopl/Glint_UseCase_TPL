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

# Data quality thresholds
MIN_DATA_QUALITY_SCORE = 0.8
MAX_NULL_PERCENTAGE = 0.2

# Logging
LOG_LEVEL = "INFO"
