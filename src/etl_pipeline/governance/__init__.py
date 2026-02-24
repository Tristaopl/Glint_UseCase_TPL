"""
Governance module for ETL pipeline
Handles data lineage tracking and quality checks
"""

from .lineage import LineageTracker
from .quality import QualityChecker

__all__ = ["LineageTracker", "QualityChecker"]
