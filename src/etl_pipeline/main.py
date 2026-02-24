"""
Main ETL Pipeline Orchestrator
Coordinates the flow through Bronze -> Silver -> Gold layers
"""
import pandas as pd
from bronze import BronzeLayer
from silver import SilverLayer
from gold import GoldLayer


class ETLPipeline:
    """Main orchestrator for the ETL pipeline"""

    def __init__(self):
        self.bronze = BronzeLayer()
        self.silver = SilverLayer()
        self.gold = GoldLayer()

    def run(self, source_data, table_name, silver_config=None, gold_config=None):
        """
        Run the complete ETL pipeline
        
        Args:
            source_data: Raw data source
            table_name: Name of the table
            silver_config: Configuration for silver layer transformations
            gold_config: Configuration for gold layer aggregations
            
        Returns:
            Tuple of (bronze_df, silver_df, gold_df)
        """
        print("=" * 60)
        print(f"🚀 Starting ETL Pipeline for: {table_name}")
        print("=" * 60)
        
        # Bronze Layer: Ingest raw data
        bronze_df = self.bronze.ingest_data(source_data, table_name)
        
        # Silver Layer: Clean and transform
        silver_df = self.silver.process(bronze_df, table_name, silver_config)
        
        # Gold Layer: Create analytics tables
        gold_df = self.gold.create_analytics_table(silver_df, table_name, gold_config)
        
        print("\n" + "=" * 60)
        print(f"✅ ETL Pipeline completed successfully!")
        print("=" * 60 + "\n")
        
        return bronze_df, silver_df, gold_df


# Example usage
if __name__ == "__main__":
    
    # Sample data
    sample_data = {
        "name": ["Alice", "Bob", "Charlie", "Alice", "David"],
        "age": [25, 30, 35, 25, None],
        "city": ["New York", "Paris", "London", "New York", "Berlin"],
        "salary": [50000, 60000, 75000, 50000, 55000]
    }
    
    # Silver layer configuration
    silver_config = {
        "type_mapping": {
            "age": "float",
            "salary": "float"
        },
        "remove_outliers": False
    }
    
    # Gold layer configuration - Example 1: Aggregation
    gold_config = {
        "group_by": ["city"],
        "aggregations": {
            "age": "mean",
            "salary": "sum"
        },
        "metrics": {
            "employee_count": lambda df: df.groupby("city").transform("count")["age"],
            "avg_salary": lambda df: df.groupby("city").transform("mean")["salary"]
        }
    }
    
    # Run the pipeline
    pipeline = ETLPipeline()
    bronze_df, silver_df, gold_df = pipeline.run(
        sample_data,
        "employees",
        silver_config=silver_config,
        gold_config=gold_config
    )
    
    # Display results
    print("\n📊 BRONZE LAYER (Raw Data):")
    print(bronze_df.head())
    
    print("\n🧹 SILVER LAYER (Cleaned Data):")
    print(silver_df.head())
    
    print("\n🏆 GOLD LAYER (Analytics Ready):")
    print(gold_df.head())
