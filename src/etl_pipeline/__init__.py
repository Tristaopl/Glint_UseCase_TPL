"""
ETL Pipeline Package
A three-layer architecture for data processing:
- Bronze: Raw data ingestion
- Silver: Data cleaning and transformation
- Gold: Business-ready analytics
"""

from bronze import BronzeLayer
from silver import SilverLayer
from gold import GoldLayer
from main import ETLPipeline

__all__ = ["BronzeLayer", "SilverLayer", "GoldLayer", "ETLPipeline"]
