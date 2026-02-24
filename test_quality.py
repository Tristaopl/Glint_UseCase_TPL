"""
Test Data Quality on Gold Layer
Runs comprehensive quality checks on star schema
"""
import sys
from pathlib import Path
import pandas as pd

# Add etl_pipeline to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "etl_pipeline"))

from governance.quality import QualityChecker
from config import GOLD_DIR


def test_data_quality():
    """Run comprehensive quality checks on gold layer"""
    
    print("\n" + "="*80)
    print("✅ DATA QUALITY TESTING - GOLD LAYER")
    print("="*80)
    
    quality = QualityChecker()
    
    # Load all gold tables
    print("\n📂 Loading gold layer tables...")
    try:
        dim_athlete = pd.read_parquet(GOLD_DIR / "dim_athlete.parquet")
        dim_country = pd.read_parquet(GOLD_DIR / "dim_country.parquet")
        dim_event = pd.read_parquet(GOLD_DIR / "dim_event.parquet")
        dim_games = pd.read_parquet(GOLD_DIR / "dim_games.parquet")
        fact = pd.read_parquet(GOLD_DIR / "fact_athlete_event_result.parquet")
        
        print(f"✅ Loaded all tables:")
        print(f"   • dim_athlete: {len(dim_athlete)} rows")
        print(f"   • dim_country: {len(dim_country)} rows")
        print(f"   • dim_event: {len(dim_event)} rows")
        print(f"   • dim_games: {len(dim_games)} rows")
        print(f"   • fact_athlete_event_result: {len(fact)} rows")
        
    except Exception as e:
        print(f"❌ Error loading tables: {e}")
        print("Run test_gold.py first to create the gold layer tables")
        return
    
    # ========================================
    # DIMENSION QUALITY CHECKS
    # ========================================
    print("\n" + "="*80)
    print("🔍 DIMENSION TABLE QUALITY CHECKS")
    print("="*80)
    
    # Athlete dimension
    print("\n📊 dim_athlete checks:")
    quality.check_row_count(dim_athlete, min_rows=100, table_name="dim_athlete")
    quality.check_null_percentage(dim_athlete, max_null_pct=0.3, table_name="dim_athlete")
    quality.check_duplicate_keys(dim_athlete, ['athlete_key'], table_name="dim_athlete")
    quality.check_duplicate_keys(dim_athlete, ['id'], table_name="dim_athlete (business key)")
    quality.check_data_type(dim_athlete, 'athlete_key', 'int64', table_name="dim_athlete")
    
    # Country dimension
    print("\n📊 dim_country checks:")
    quality.check_row_count(dim_country, min_rows=1, table_name="dim_country")
    quality.check_null_percentage(dim_country, max_null_pct=0.5, table_name="dim_country")
    quality.check_duplicate_keys(dim_country, ['country_key'], table_name="dim_country")
    quality.check_duplicate_keys(dim_country, ['noc'], table_name="dim_country (business key)")
    quality.check_data_type(dim_country, 'country_key', 'int64', table_name="dim_country")
    
    # Event dimension
    print("\n📊 dim_event checks:")
    quality.check_row_count(dim_event, min_rows=1, table_name="dim_event")
    quality.check_null_percentage(dim_event, max_null_pct=0.0, table_name="dim_event")
    quality.check_duplicate_keys(dim_event, ['event_key'], table_name="dim_event")
    quality.check_data_type(dim_event, 'event_key', 'int64', table_name="dim_event")
    
    # Games dimension
    print("\n📊 dim_games checks:")
    quality.check_row_count(dim_games, min_rows=1, table_name="dim_games")
    quality.check_null_percentage(dim_games, max_null_pct=0.0, table_name="dim_games")
    quality.check_duplicate_keys(dim_games, ['games_key'], table_name="dim_games")
    quality.check_data_type(dim_games, 'games_key', 'int64', table_name="dim_games")
    
    # ========================================
    # FACT TABLE QUALITY CHECKS
    # ========================================
    print("\n" + "="*80)
    print("🔍 FACT TABLE QUALITY CHECKS")
    print("="*80)
    
    print("\n📊 fact_athlete_event_result checks:")
    quality.check_row_count(fact, min_rows=1, table_name="fact_athlete_event_result")
    quality.check_null_percentage(fact, max_null_pct=0.5, table_name="fact_athlete_event_result")
    quality.check_duplicate_keys(fact, ['result_key'], table_name="fact_athlete_event_result")
    quality.check_data_type(fact, 'result_key', 'int64', table_name="fact_athlete_event_result")
    quality.check_data_type(fact, 'medal_flag', 'int64', table_name="fact_athlete_event_result")
    
    # ========================================
    # REFERENTIAL INTEGRITY CHECKS
    # ========================================
    print("\n" + "="*80)
    print("🔗 REFERENTIAL INTEGRITY CHECKS")
    print("="*80)
    
    print("\n📊 Foreign key relationships:")
    quality.check_referential_integrity(
        fact, dim_athlete, 'athlete_key', 'athlete_key',
        fact_table="fact", dim_table="dim_athlete"
    )
    quality.check_referential_integrity(
        fact, dim_country, 'country_key', 'country_key',
        fact_table="fact", dim_table="dim_country"
    )
    quality.check_referential_integrity(
        fact, dim_event, 'event_key', 'event_key',
        fact_table="fact", dim_table="dim_event"
    )
    quality.check_referential_integrity(
        fact, dim_games, 'games_key', 'games_key',
        fact_table="fact", dim_table="dim_games"
    )
    
    # ========================================
    # QUALITY SUMMARY & REPORT
    # ========================================
    print("\n" + "="*80)
    print("📋 QUALITY REPORT SUMMARY")
    print("="*80)
    
    quality.print_quality_summary()
    
    report = quality.get_quality_report()
    
    # Export detailed report
    if report['checks']:
        df_report = pd.DataFrame(report['checks'])
        report_file = GOLD_DIR / "quality_report.csv"
        df_report.to_csv(report_file, index=False)
        print(f"\n📄 Detailed report saved to: {report_file}")
    
    # Assert quality meets threshold
    print("\n" + "="*80)
    print("🎯 QUALITY ASSERTION")
    print("="*80)
    
    if quality.assert_quality(min_pass_rate=0.90):
        print("\n✅ QUALITY THRESHOLDS MET (90% pass rate)")
        print("="*80)
        print("✅ ALL QUALITY CHECKS PASSED!")
        print("="*80)
        return True
    else:
        print("\n❌ QUALITY BELOW THRESHOLD")
        print(f"   Required: 90% pass rate")
        print(f"   Actual: {report['pass_rate']*100:.1f}%")
        print("\nFailed checks:")
        for failure in report['failures']:
            print(f"   • {failure}")
        return False


if __name__ == "__main__":
    success = test_data_quality()
    sys.exit(0 if success else 1)
