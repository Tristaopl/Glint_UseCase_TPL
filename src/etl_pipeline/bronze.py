import pandas as pd
from datetime import datetime
from pathlib import Path
from config import BRONZE_DIR  # should be a Path object, e.g., Path("/data/bronze")

class BronzeLayer:
    """Bronze Layer for CSV ingestion"""

    def __init__(self, bronze_dir=BRONZE_DIR):
        self.bronze_dir = Path(bronze_dir)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)  # ensure folder exists

    def ingest_csv(self, csv_path, table_name):
        """
        Load CSV and save to bronze layer with timestamped filename.
        
        Args:
            csv_path: Path to the source CSV file
            table_name: Name of the data table
        
        Returns:
            pd.DataFrame: Loaded DataFrame
        """
        print(f"📥 Loading CSV for table: {table_name}")
        df = pd.read_csv(csv_path)

        # Add metadata
        df["_ingestion_timestamp"] = datetime.now()
        df["_source_system"] = "raw_ingestion"

        # Build target filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_file = self.bronze_dir / f"{table_name}_{timestamp}.csv"

        df.to_csv(target_file, index=False)
        print(f"💾 Saved bronze CSV to: {target_file} ({len(df)} rows, {len(df.columns)} columns)")

        return df