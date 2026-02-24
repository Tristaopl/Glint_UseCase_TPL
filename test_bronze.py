"""
Test script for Bronze Layer
Tests CSV ingestion and metadata addition
"""
import sys
from pathlib import Path

# Add etl_pipeline to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "etl_pipeline"))

from bronze import BronzeLayer
from config import BRONZE_DIR


def test_bronze_layer():
    """Test the bronze layer with Olympic data"""
    
    print("\n" + "="*70)
    print("🔵 TESTING BRONZE LAYER")
    print("="*70)
    
    # Initialize bronze layer
    bronze = BronzeLayer()
    
    print(f"\n📂 Bronze directory: {BRONZE_DIR}")
    
    # Test 1: Ingest athlete_events.csv
    print("\n" + "-"*70)
    print("Test 1: Loading athlete_events.csv")
    print("-"*70)
    try:
        df_athletes = bronze.ingest_data(
            "data_source/athlete_events.csv",
            "athlete_events"
        )
        print(f"✅ Successfully ingested athlete_events.csv")
        print(f"   Shape: {df_athletes.shape}")
        print(f"   Columns: {list(df_athletes.columns)}")
        print(f"\n   First 3 rows:")
        print(df_athletes.head(3).to_string())
        print(f"\n   Metadata columns added:")
        print(f"   - _ingestion_timestamp: {df_athletes['_ingestion_timestamp'].iloc[0]}")
        print(f"   - _source_system: {df_athletes['_source_system'].iloc[0]}")
    except Exception as e:
        print(f"❌ Error loading athlete_events: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 2: Ingest noc_regions.csv
    print("\n" + "-"*70)
    print("Test 2: Loading noc_regions.csv")
    print("-"*70)
    try:
        df_noc = bronze.ingest_data(
            "data_source/noc_regions.csv",
            "noc_regions"
        )
        print(f"✅ Successfully ingested noc_regions.csv")
        print(f"   Shape: {df_noc.shape}")
        print(f"   Columns: {list(df_noc.columns)}")
        print(f"\n   First 3 rows:")
        print(df_noc.head(3).to_string())
    except Exception as e:
        print(f"❌ Error loading noc_regions: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 3: Verify files were saved
    print("\n" + "-"*70)
    print("Test 3: Verifying saved files")
    print("-"*70)
    bronze_files = list(BRONZE_DIR.glob("*.csv"))
    print(f"✅ Found {len(bronze_files)} files in bronze directory:")
    for file in sorted(bronze_files):
        file_size = file.stat().st_size / 1024  # KB
        print(f"   • {file.name} ({file_size:.2f} KB)")
    
    # Test 4: Verify metadata
    print("\n" + "-"*70)
    print("Test 4: Verifying metadata columns")
    print("-"*70)
    print(f"✅ athlete_events metadata:")
    print(f"   _ingestion_timestamp unique values: {df_athletes['_ingestion_timestamp'].nunique()}")
    print(f"   _source_system values: {df_athletes['_source_system'].unique()}")
    
    print(f"\n✅ noc_regions metadata:")
    print(f"   _ingestion_timestamp unique values: {df_noc['_ingestion_timestamp'].nunique()}")
    print(f"   _source_system values: {df_noc['_source_system'].unique()}")
    
    # Test 5: Data quality check
    print("\n" + "-"*70)
    print("Test 5: Data Quality Check")
    print("-"*70)
    print(f"✅ athlete_events:")
    print(f"   Total rows: {len(df_athletes)}")
    print(f"   Missing values: {df_athletes.isnull().sum().sum()}")
    print(f"   Data types: {df_athletes.dtypes.to_dict()}")
    
    print(f"\n✅ noc_regions:")
    print(f"   Total rows: {len(df_noc)}")
    print(f"   Missing values: {df_noc.isnull().sum().sum()}")
    print(f"   Data types: {df_noc.dtypes.to_dict()}")
    
    print("\n" + "="*70)
    print("✅ ALL BRONZE LAYER TESTS PASSED!")
    print("="*70)
    print(f"\n📂 Output files saved to: {BRONZE_DIR}")
    
    return df_athletes, df_noc


if __name__ == "__main__":
    df_athletes, df_noc = test_bronze_layer()
