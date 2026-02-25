import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from config import SILVER_DIR, FILE_FORMAT

class SilverLayer:
    """Handles data cleaning, validation, and transformation"""

    def __init__(self):
        self.silver_dir = SILVER_DIR
        self.file_format = FILE_FORMAT
        self.quality_issues = []

    def remove_duplicates(self, df, subset=None):
        """Remove duplicate rows"""
        initial_count = len(df)
        df = df.drop_duplicates(subset=subset, keep="first")
        removed = initial_count - len(df)
        if removed > 0:
            print(f"  🔄 Removed {removed} duplicate rows")
            self.quality_issues.append(f"Duplicates: {removed} rows removed")
        return df

    def handle_missing_values(self, df, strategy="drop"):
        """
        Handle missing values
        
        Args:
            df: Input DataFrame
            strategy: 'drop', 'fillna_mean', 'fillna_forward', 'fillna_value'
        """
        missing_count = df.isnull().sum().sum()
        if missing_count > 0:
            print(f"  📊 Found {missing_count} missing values")
            
            if strategy == "drop":
                df = df.dropna()
            elif strategy == "fillna_mean":
                df = df.fillna(df.mean(numeric_only=True))
            elif strategy == "fillna_forward":
                df = df.fillna(method="ffill")
            
            self.quality_issues.append(f"Missing values: {missing_count} handled with {strategy}")
        
        return df

    def standardize_column_names(self, df):
        """Standardize column names: lowercase, remove spaces"""
        df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
        print("  🏷️  Standardized column names")
        return df

    def validate_data_types(self, df, type_mapping=None):
        """Validate and convert data types"""
        if type_mapping:
            for column, dtype in type_mapping.items():
                if column in df.columns:
                    try:
                        df[column] = df[column].astype(dtype)
                    except Exception as e:
                        print(f"  ⚠️  Warning: Could not convert {column} to {dtype}: {e}")
        
        print("  ✓ Data types validated")
        return df

    def remove_outliers(self, df, columns=None, z_score_threshold=3):
        """Remove outliers using z-score method"""
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns
        
        initial_count = len(df)
        
        for column in columns:
            if column in df.columns:
                z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
                df = df[z_scores < z_score_threshold]
        
        removed = initial_count - len(df)
        if removed > 0:
            print(f"  📈 Removed {removed} outlier rows")
            self.quality_issues.append(f"Outliers: {removed} rows removed")
        
        return df

    def add_processing_metadata(self, df):
        """Add metadata columns"""
        df["_processing_timestamp"] = datetime.now()
        df["_layer"] = "silver"
        return df

    def transform_data(self, df, transformations=None):
        """
        Apply custom transformations
        
        Args:
            df: Input DataFrame
            transformations: List of callable functions to apply
        """
        if transformations:
            for transform_func in transformations:
                df = transform_func(df)
        
        return df

    def clean_and_transform(self, df, config=None):
        """Complete cleaning and transformation pipeline"""
        print(f"\n🧹 [SILVER] Cleaning and transforming data...")
        
        # Standard cleaning steps
        df = self.standardize_column_names(df)
        df = self.remove_duplicates(df)
        
        # Get missing value strategy from config or use default
        missing_strategy = "drop"
        if config and "missing_strategy" in config:
            missing_strategy = config["missing_strategy"]
        df = self.handle_missing_values(df, strategy=missing_strategy)
        
        # Optional transformations from config
        if config:
            if "type_mapping" in config:
                df = self.validate_data_types(df, config["type_mapping"])
            if "remove_outliers" in config and config["remove_outliers"]:
                df = self.remove_outliers(df)
            if "transformations" in config:
                df = self.transform_data(df, config["transformations"])
        
        df = self.add_processing_metadata(df)
        
        print(f"✓ Cleaned data: {len(df)} rows, {len(df.columns)} columns")
        return df

    def save_silver_data(self, df, table_name):
        """Save cleaned data to silver layer"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_cleaned_{timestamp}.{self.file_format}"
        filepath = self.silver_dir / filename

        if self.file_format == "csv":
            df.to_csv(filepath, index=False)
        elif self.file_format == "parquet":
            df.to_parquet(filepath, index=False)
        elif self.file_format == "json":
            df.to_json(filepath)

        print(f"💾 Saved silver data to: {filepath}")
        return filepath

    def process(self, df, table_name, config=None):
        """Complete silver layer processing pipeline"""
        df = self.clean_and_transform(df, config)
        self.save_silver_data(df, table_name)
        
        print(f"\nQuality Issues Found: {len(self.quality_issues)}")
        for issue in self.quality_issues:
            print(f"  • {issue}")
        
        return df
