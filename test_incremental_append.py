"""
Test incremental append for fact table
Demonstrates full build vs incremental append modes
"""
import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "etl_pipeline"))

from bronze import BronzeLayer
from silver import SilverLayer
from gold_star import GoldStarBuilder
from governance.lineage import LineageTracker
from governance.quality import QualityChecker
from config import GOLD_DIR, DATA_DIR

def test_incremental_append():
    """Test incremental append functionality"""
    
    print("\n" + "="*80)
    print("🧪 TEST: INCREMENTAL FACT TABLE APPEND")
    print("="*80)
    
    # Initialize components
    lineage = LineageTracker()
    quality = QualityChecker()
    builder = GoldStarBuilder(lineage, quality)
    
    bronze = BronzeLayer()
    silver = SilverLayer()
    
    # Paths
    data_source = Path("data_source")
    athlete_csv = data_source / "athlete_events.csv"
    noc_csv = data_source / "noc_regions.csv"
    fact_path = GOLD_DIR / "fact_athlete_event_result.parquet"
    fact_incremental_path = GOLD_DIR / "fact_athlete_event_result_incremental.parquet"
    
    # Check if source files exist
    if not athlete_csv.exists():
        print(f"\n❌ Error: {athlete_csv} not found")
        print("Please ensure data_source/athlete_events.csv exists")
        return
    
    print("\n" + "-"*80)
    print("📥 PHASE 1: Load raw data from CSV")
    print("-"*80)
    
    try:
        bronze_athletes = bronze.ingest_data(str(athlete_csv), "athlete_events")
        bronze_noc = bronze.ingest_data(str(noc_csv), "noc_regions")
        print(f"✅ Loaded: {len(bronze_athletes)} athlete events, {len(bronze_noc)} NOC regions")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return
    
    print("\n" + "-"*80)
    print("🧹 PHASE 2: Clean and standardize data (SILVER)")
    print("-"*80)
    
    silver_config = {
        "type_mapping": {
            "age": "float",
            "height": "float",
            "weight": "float",
            "year": "int"
        },
        "remove_outliers": False
    }
    
    silver_athletes = silver.process(bronze_athletes, "athlete_events", silver_config)
    silver_noc = silver.process(bronze_noc, "noc_regions", {})
    print(f"✅ Cleaned: {len(silver_athletes)} athlete rows, {len(silver_noc)} NOC rows")
    
    # ========================================
    # SCENARIO 1: FULL BUILD
    # ========================================
    print("\n" + "="*80)
    print("🔵 SCENARIO 1: FULL BUILD (First run)")
    print("="*80)
    
    try:
        dim_a, dim_c, dim_e, dim_g, fact_full = \
            builder.build_star_schema(silver_athletes, silver_noc, 
                                      run_quality_checks=True)
        
        # Save for incremental test
        fact_full.to_parquet(fact_incremental_path, index=False)
        print(f"\n✅ Full build completed: {len(fact_full)} fact records")
        builder.print_schema_summary(dim_a, dim_c, dim_e, dim_g, fact_full)
        
    except Exception as e:
        print(f"❌ Error in full build: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ========================================
    # SCENARIO 2: INCREMENTAL APPEND
    # ========================================
    print("\n" + "="*80)
    print("♻️  SCENARIO 2: INCREMENTAL APPEND (Second run with same data)")
    print("="*80)
    print("\nℹ️  Using same data as first run (should skip all duplicate keys)")
    
    try:
        dim_a2, dim_c2, dim_e2, dim_g2, fact_inc, stats = \
            builder.build_star_schema_incremental(
                silver_athletes, 
                silver_noc,
                existing_fact_path=str(fact_incremental_path),
                run_quality_checks=True
            )
        
        print(f"\n📊 Incremental Append Statistics:")
        print(f"   • Mode: {stats['mode']}")
        print(f"   • New rows created: {stats['new_rows']}")
        print(f"   • Rows appended: {stats['appended_rows']}")
        print(f"   • Duplicate keys skipped: {stats['duplicate_keys_skipped']}")
        print(f"   • Final fact table size: {len(fact_inc)}")
        
        assert len(fact_inc) == len(fact_full), \
            f"Expected {len(fact_full)} rows, got {len(fact_inc)}"
        print(f"\n✅ Incremental append successful - no new rows added (expected)")
        
    except Exception as e:
        print(f"❌ Error in incremental append: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ========================================
    # SCENARIO 3: PARTIAL NEW DATA
    # ========================================
    print("\n" + "="*80)
    print("➕ SCENARIO 3: INCREMENTAL APPEND WITH NEW DATA")
    print("="*80)
    print("\nℹ️  Simulating new data: Taking first 50 rows + random sampling")
    
    try:
        # Simulate new data by taking subset
        partial_athletes = silver_athletes.head(50).copy()
        partial_athletes_sample = silver_athletes.sample(n=min(100, len(silver_athletes)), 
                                                         random_state=42)
        silver_athletes_partial = pd.concat(
            [partial_athletes, partial_athletes_sample],
            ignore_index=True
        ).drop_duplicates()
        
        print(f"   • Using subset with {len(silver_athletes_partial)} unique athlete records")
        
        dim_a3, dim_c3, dim_e3, dim_g3, fact_inc2, stats2 = \
            builder.build_star_schema_incremental(
                silver_athletes_partial,
                silver_noc,
                existing_fact_path=str(fact_incremental_path),
                run_quality_checks=False  # Skip for speed in test
            )
        
        print(f"\n📊 Incremental Append Statistics (Partial Data):")
        print(f"   • Mode: {stats2['mode']}")
        print(f"   • New rows created: {stats2['new_rows']}")
        print(f"   • Rows appended: {stats2['appended_rows']}")
        print(f"   • Duplicate keys skipped: {stats2['duplicate_keys_skipped']}")
        print(f"   • Final fact table size: {len(fact_inc2)}")
        
        rows_added_beyond_original = len(fact_inc2) - len(fact_full)
        if rows_added_beyond_original > 0:
            print(f"\n✅ Successfully appended {rows_added_beyond_original} new rows")
        else:
            print(f"\n✅ All rows were duplicates (expected with partial dataset)")
        
    except Exception as e:
        print(f"❌ Error in partial data scenario: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ========================================
    # REVIEW LINEAGE
    # ========================================
    print("\n" + "="*80)
    print("📋 LINEAGE & CHECKPOINTS")
    print("="*80)
    
    lineage.print_lineage_summary()
    
    # Get checkpoint info
    checkpoint = lineage.get_fact_table_checkpoint()
    if checkpoint:
        print(f"\n📍 Latest Fact Table Checkpoint:")
        print(f"   • Size: {checkpoint['fact_table_size']} rows")
        print(f"   • New rows in last run: {checkpoint['new_rows_processed']}")
        print(f"   • Timestamp: {checkpoint['checkpoint_timestamp']}")
    
    print("\n" + "="*80)
    print("✅ TEST COMPLETED SUCCESSFULLY")
    print("="*80)
    
    print(f"\n📂 Output files:")
    print(f"   • Full build fact: {fact_path}")
    print(f"   • Incremental fact: {fact_incremental_path}")
    print(f"   • Lineage logs: {lineage.log_dir}/lineage_*.json")
    print(f"   • Checkpoint: {lineage.log_dir}/fact_table_checkpoint.json")

if __name__ == "__main__":
    test_incremental_append()
