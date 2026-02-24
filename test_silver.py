                                                                                                                                                                                                                                                                                                                                                                                        """
Test script for Silver Layer
Tests data cleaning and transformation
"""
import sys
from pathlib import Path

# Add etl_pipeline to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "etl_pipeline"))

from silver import SilverLayer
from bronze import BronzeLayer
from config import SILVER_DIR, BRONZE_DIR
import pandas as pd


def test_silver_layer():
    """Test the silver layer with Olympic data"""
    
    print("\n" + "="*70)
    print("⚪ TESTING SILVER LAYER")
    print("="*70)
    
    # Initialize layers
    bronze = BronzeLayer()
    silver = SilverLayer()
    
    print(f"\n📂 Silver directory: {SILVER_DIR}")
    
    # Test 1: Load bronze data for athlete_events
    print("\n" + "-"*70)
    print("Test 1: Loading athlete_events from bronze")
    print("-"*70)
    try:
        # Find the most recent bronze file
        bronze_files = sorted(BRONZE_DIR.glob("athlete_events*.parquet"))
        if not bronze_files:
            print("❌ No bronze files found. Run test_bronze.py first!")
            return
        
        latest_bronze = bronze_files[-1]
        print(f"📂 Loading: {latest_bronze.name}")
        df_athletes = pd.read_parquet(latest_bronze)
        print(f"✅ Loaded {len(df_athletes)} rows, {len(df_athletes.columns)} columns")
    except Exception as e:
        print(f"❌ Error loading bronze file: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 2: Clean and transform athlete_events
    print("\n" + "-"*70)
    print("Test 2: Cleaning athlete_events data")
    print("-"*70)
    
    # Define cleaning configuration
    silver_config = {
        "type_mapping": {
            "age": "float",
            "height": "float",
            "weight": "float",
            "year": "int"
        },
        "remove_outliers": False  # Set to True to remove statistical outliers
    }
    
    try:
        df_athletes_clean = silver.process(
            df_athletes,
            "athlete_events",
            config=silver_config
        )
        print(f"\n✅ Successfully cleaned athlete_events")
        print(f"   Original: {len(df_athletes)} rows")
        print(f"   Cleaned:  {len(df_athletes_clean)} rows")
        print(f"   Rows removed: {len(df_athletes) - len(df_athletes_clean)}")
    except Exception as e:
        print(f"❌ Error cleaning data: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 3: Load and clean noc_regions
    print("\n" + "-"*70)
    print("Test 3: Cleaning noc_regions data")
    print("-"*70)
    
    try:
        bronze_files = sorted(BRONZE_DIR.glob("noc_regions*.parquet"))
        if not bronze_files:
            print("❌ No bronze files found for noc_regions")
            return
        
        latest_bronze = bronze_files[-1]
        print(f"📂 Loading: {latest_bronze.name}")
        df_noc = pd.read_parquet(latest_bronze)
        print(f"✅ Loaded {len(df_noc)} rows, {len(df_noc.columns)} columns")
        
        # Clean noc_regions (minimal config needed)
        noc_config = {}
        df_noc_clean = silver.process(
            df_noc,
            "noc_regions",
            config=noc_config
        )
        print(f"\n✅ Successfully cleaned noc_regions")
        print(f"   Original: {len(df_noc)} rows")
        print(f"   Cleaned:  {len(df_noc_clean)} rows")
    except Exception as e:
        print(f"❌ Error cleaning noc_regions: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 4: Verify files were saved
    print("\n" + "-"*70)
    print("Test 4: Verifying saved files")
    print("-"*70)
    silver_files = list(SILVER_DIR.glob("*.parquet"))
    print(f"✅ Found {len(silver_files)} files in silver directory:")
    for file in sorted(silver_files):
        file_size = file.stat().st_size / 1024  # KB
        print(f"   • {file.name} ({file_size:.2f} KB)")
    
    # Test 5: Data quality summary
    print("\n" + "-"*70)
    print("Test 5: Data Quality Summary")
    print("-"*70)
    print(f"✅ athlete_events after cleaning:")
    print(f"   Rows: {len(df_athletes_clean)}")
    print(f"   Columns: {len(df_athletes_clean.columns)}")
    print(f"   Missing values: {df_athletes_clean.isnull().sum().sum()}")
    print(f"   Sample columns: {df_athletes_clean.columns[:5].tolist()}")
    
    print(f"\n✅ noc_regions after cleaning:")
    print(f"   Rows: {len(df_noc_clean)}")
    print(f"   Columns: {len(df_noc_clean.columns)}")
    print(f"   Missing values: {df_noc_clean.isnull().sum().sum()}")
    
    print("\n" + "="*70)
    print("✅ ALL SILVER LAYER TESTS PASSED!")
    print("="*70)
    print(f"\n📂 Output files saved to: {SILVER_DIR}")
    print("\n💡 Next: Run test_gold.py to create business analytics")
    
    return df_athletes_clean, df_noc_clean


if __name__ == "__main__":
    test_silver_layer()
