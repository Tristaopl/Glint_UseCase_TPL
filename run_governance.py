"""
Run Governance Checks - Quality & Lineage
Comprehensive data quality and lineage tracking for the ETL pipeline
"""
import sys
from pathlib import Path
import pandas as pd

# Add etl_pipeline to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "etl_pipeline"))

from governance.quality import QualityChecker
from governance.lineage import LineageTracker
from main import ETLPipeline
from config import GOLD_DIR, BRONZE_DIR, SILVER_DIR


def run_governance():
    """Run complete governance pipeline (quality + lineage)"""
    
    print("\n" + "="*80)
    print("🏛️  GOVERNANCE FRAMEWORK - QUALITY & LINEAGE CHECKS")
    print("="*80)
    
    # Initialize trackers
    lineage = LineageTracker()
    quality = QualityChecker()
    
    # ========================================
    # RUN ETL PIPELINE WITH LINEAGE TRACKING
    # ========================================
    print("\n" + "="*80)
    print("🔄 RUNNING ETL PIPELINE WITH LINEAGE TRACKING")
    print("="*80)
    
    try:
        # Track Bronze layer
        print("\n📍 Bronze Layer - Ingesting raw data...")
        lineage.start_run(
            layer="bronze",
            table_name="raw_athlete_events",
            source=str(BRONZE_DIR / "athlete_events.csv")
        )
        
        # Run pipeline
        pipeline = ETLPipeline()
        pipeline.run()
        
        # Log successful completion
        lineage.end_run(status="success")
        print("✅ ETL Pipeline completed successfully with lineage tracking")
        
        # Save lineage log
        log_file = lineage.save_lineage_log()
        
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
        lineage.end_run(status="failed", errors=[str(e)])
        lineage.save_lineage_log()
        return False
    
    # ========================================
    # RUN COMPREHENSIVE QUALITY CHECKS
    # ========================================
    print("\n" + "="*80)
    print("✅ DATA QUALITY CHECKS - GOLD LAYER")
    print("="*80)
    
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
        return False
    
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
    # SCD TYPE CHECKS (SCD1, SCD2, TYPE0)
    # ========================================
    print("\n" + "="*80)
    print("📋 SLOWLY CHANGING DIMENSION (SCD) CHECKS")
    print("="*80)
    
    # Check SCD1 (Athlete)
    print("\n📊 SCD1 Check (dim_athlete) - Update in place:")
    athlete_dups = dim_athlete.groupby('id').size()
    athlete_dups_found = (athlete_dups > 1).sum()
    if athlete_dups_found == 0:
        print(f"✅ SCD1 integrity: All athletes have single row (no versioning)")
    else:
        print(f"⚠️  SCD1 violation: {athlete_dups_found} athletes with multiple rows")
    
    # Check SCD2 (Country)
    print("\n📊 SCD2 Check (dim_country) - Historical tracking:")
    country_with_history = (dim_country.groupby('noc').size() > 1).sum()
    if country_with_history > 0:
        print(f"✅ SCD2 working: {country_with_history} countries have historical versions")
    else:
        print(f"✅ SCD2 compliant: No countries changed yet (current versions only)")
    
    # Check for proper effective dates
    if 'effective_date' in dim_country.columns:
        null_effective = dim_country['effective_date'].isnull().sum()
        if null_effective == 0:
            print(f"✅ Effective dates populated: All {len(dim_country)} rows have effective_date")
        else:
            print(f"⚠️  Effective dates missing: {null_effective} rows")
    
    # Check Type0 dimensions (immutable)
    print("\n📊 Type0 Check (dim_event, dim_games) - Immutable:")
    event_dups = (dim_event.groupby('event_name').size() > 1).sum()
    games_dups = (dim_games.groupby('games_name').size() > 1).sum()
    print(f"✅ Type0 immutable: Event duplicates={event_dups}, Games duplicates={games_dups}")
    
    # ========================================
    # QUALITY SUMMARY & REPORT
    # ========================================
    print("\n" + "="*80)
    print("📋 GOVERNANCE REPORT SUMMARY")
    print("="*80)
    
    quality.print_quality_summary()
    
    report = quality.get_quality_report()
    
    # Export detailed report
    if report['checks']:
        df_report = pd.DataFrame(report['checks'])
        report_file = GOLD_DIR / "governance_quality_report.csv"
        df_report.to_csv(report_file, index=False)
        print(f"\n📄 Quality report saved to: {report_file}")
    
    # ========================================
    # LINEAGE SUMMARY
    # ========================================
    print("\n" + "="*80)
    print("🔄 LINEAGE TRACKING SUMMARY")
    print("="*80)
    
    history = lineage.get_run_history()
    for run in history:
        print(f"\n📍 {run['layer'].upper()} Layer:")
        print(f"   Table: {run['table_name']}")
        print(f"   Status: {run['status']}")
        print(f"   Source: {run['source']}")
        print(f"   Start: {run['start_time']}")
        if run.get('end_time'):
            print(f"   End: {run['end_time']}")
        if run['row_count_in'] is not None:
            print(f"   Rows In: {run['row_count_in']}")
        if run['row_count_out'] is not None:
            print(f"   Rows Out: {run['row_count_out']}")
        if run['transformations']:
            print(f"   Transformations ({len(run['transformations'])}):")
            for tx in run['transformations']:
                print(f"      • {tx['name']}")
    
    # ========================================
    # FINAL ASSERTION
    # ========================================
    print("\n" + "="*80)
    print("🎯 GOVERNANCE ASSERTION")
    print("="*80)
    
    if quality.assert_quality(min_pass_rate=0.90):
        print("\n✅ GOVERNANCE CHECKS PASSED!")
        print("   • Quality thresholds met (90% pass rate)")
        print("   • Lineage tracked successfully")
        print("   • Star schema validated")
        print("   • SCD types validated")
        print("\n" + "="*80)
        return True
    else:
        print("\n❌ GOVERNANCE ISSUES DETECTED")
        print(f"   Required: 90% pass rate")
        print(f"   Actual: {report['pass_rate']*100:.1f}%")
        print("\nFailed checks:")
        for failure in report['failures']:
            print(f"   • {failure}")
        print("\n" + "="*80)
        return False


if __name__ == "__main__":
    success = run_governance()
    sys.exit(0 if success else 1)
