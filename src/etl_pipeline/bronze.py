import pandas as pd
from datetime import datetime
from pathlib import Path
from config import BRONZE_DIR, FILE_FORMAT  # Import FILE_FORMAT from config

class BronzeLayer:
    """Bronze Layer for CSV ingestion"""

    def __init__(self, bronze_dir=BRONZE_DIR):
        self.bronze_dir = Path(bronze_dir)
        self.file_format = FILE_FORMAT
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

        # Build target filename with configurable format
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_file = self.bronze_dir / f"{table_name}_{timestamp}.{self.file_format}"

        # Save with configured format
        if self.file_format == "parquet":
            df.to_parquet(target_file, index=False)
        elif self.file_format == "json":
            df.to_json(target_file)
        else:
            df.to_csv(target_file, index=False)
        
        print(f"💾 Saved bronze {self.file_format.upper()} to: {target_file} ({len(df)} rows, {len(df.columns)} columns)")

        return df

    def ingest_data(self, source, table_name):
        """
        Compatibility wrapper to ingest data from multiple source types.

        Args:
            source: Path to CSV file or a pandas DataFrame or iterable of dicts
            table_name: Name of the table to save in bronze

        Returns:
            pd.DataFrame
        """
        # If a DataFrame is provided, save it directly
        if isinstance(source, pd.DataFrame):
            df = source.copy()
            df["_ingestion_timestamp"] = datetime.now()
            df["_source_system"] = "raw_ingestion"

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            target_file = self.bronze_dir / f"{table_name}_{timestamp}.{self.file_format}"
            
            if self.file_format == "parquet":
                df.to_parquet(target_file, index=False)
            elif self.file_format == "json":
                df.to_json(target_file)
            else:
                df.to_csv(target_file, index=False)
            
            print(f"💾 Saved bronze {self.file_format.upper()} to: {target_file} ({len(df)} rows, {len(df.columns)} columns)")
            return df

        # If a path-like string is provided, assume CSV and call ingest_csv
        try:
            src_path = Path(source)
            if src_path.exists():
                return self.ingest_csv(str(src_path), table_name)
        except Exception:
            pass

        # If source is an iterable of dicts or list of rows, convert to DataFrame
        try:
            df = pd.DataFrame(source)
            if not df.empty:
                return self.ingest_data(df, table_name)
        except Exception:
            pass

        raise ValueError("Unsupported source type for ingest_data(): provide CSV path or DataFrame or iterable of dicts")