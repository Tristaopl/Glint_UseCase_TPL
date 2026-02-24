"""
ETL Pipeline for Olympic Games Data
Processes raw athlete events and creates business analytics
Run individual layers or the complete pipeline
"""
import pandas as pd
import sys
from pathlib import Path

# Add etl_pipeline to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "etl_pipeline"))

from main import ETLPipeline
from config import BRONZE_DIR, SILVER_DIR, GOLD_DIR


# Configuration for each data source
ATHLETE_EVENTS_SILVER_CONFIG = {
    "type_mapping": {
        "age": "float",
        "height": "float",
        "weight": "float",
        "year": "int"
    },
    "remove_outliers": True
}

ATHLETE_EVENTS_GOLD_CONFIG = {
    "group_by": ["noc", "sport"],
    "aggregations": {
        "medal": "count",
        "age": "mean",
        "weight": "mean",
        "height": "mean"
    },
    "metrics": {
        "avg_age": lambda df: df.groupby(["noc", "sport"])["age"].transform("mean"),
        "athlete_count": lambda df: df.groupby(["noc", "sport"]).transform("count")["id"]
    }
}

NOC_REGIONS_SILVER_CONFIG = {
    "remove_outliers": False
}


# ===== BRONZE LAYER FUNCTIONS =====
def run_bronze_layer():
    """Run only the Bronze layer (raw data ingestion)"""
    print("\n" + "="*70)
    print("🔵 BRONZE LAYER - RAW DATA INGESTION")
    print("="*70)
    
    pipeline = ETLPipeline()
    
    print("\n📥 Loading athlete_events.csv...")
    bronze_athletes = pipeline.bronze.ingest_data(
        "data_source/athlete_events.csv",
        "athlete_events"
    )
    
    print("\n📥 Loading noc_regions.csv...")
    bronze_noc = pipeline.bronze.ingest_data(
        "data_source/noc_regions.csv",
        "noc_regions"
    )
    
    print("\n" + "="*70)
    print("✅ BRONZE LAYER COMPLETED")
    print("="*70)
    print(f"\nBronze files saved to: {BRONZE_DIR}")
    
    return bronze_athletes, bronze_noc


# ===== SILVER LAYER FUNCTIONS =====
def run_silver_layer(bronze_athletes=None, bronze_noc=None):
    """Run only the Silver layer (data cleaning and transformation)"""
    print("\n" + "="*70)
    print("⚪ SILVER LAYER - DATA CLEANING & TRANSFORMATION")
    print("="*70)
    
    pipeline = ETLPipeline()
    
    # Load bronze data if not provided
    if bronze_athletes is None:
        print("\n📂 Loading athlete_events from bronze...")
        bronze_athletes = pd.read_csv(list(BRONZE_DIR.glob("athlete_events*.csv"))[0])
    
    if bronze_noc is None:
        print("📂 Loading noc_regions from bronze...")
        bronze_noc = pd.read_csv(list(BRONZE_DIR.glob("noc_regions*.csv"))[0])
    
    print("\n🧹 Cleaning athlete_events data...")
    silver_athletes = pipeline.silver.process(
        bronze_athletes,
        "athlete_events",
        config=ATHLETE_EVENTS_SILVER_CONFIG
    )
    
    print("\n🧹 Cleaning noc_regions data...")
    silver_noc = pipeline.silver.process(
        bronze_noc,
        "noc_regions",
        config=NOC_REGIONS_SILVER_CONFIG
    )
    
    print("\n" + "="*70)
    print("✅ SILVER LAYER COMPLETED")
    print("="*70)
    print(f"\nSilver files saved to: {SILVER_DIR}")
    
    return silver_athletes, silver_noc


# ===== GOLD LAYER FUNCTIONS =====
def run_gold_layer(silver_athletes=None, silver_noc=None):
    """Run only the Gold layer (business analytics)"""
    print("\n" + "="*70)
    print("🟡 GOLD LAYER - BUSINESS ANALYTICS")
    print("="*70)
    
    pipeline = ETLPipeline()
    
    # Load silver data if not provided
    if silver_athletes is None:
        print("\n📂 Loading athlete_events from silver...")
        silver_athletes = pd.read_csv(list(SILVER_DIR.glob("athlete_events*.csv"))[0])
    
    if silver_noc is None:
        print("📂 Loading noc_regions from silver...")
        silver_noc = pd.read_csv(list(SILVER_DIR.glob("noc_regions*.csv"))[0])
    
    print("\n📊 Creating gold layer analytics...")
    gold_athletes = pipeline.gold.create_analytics_table(
        silver_athletes,
        "athlete_events_analytics",
        config=ATHLETE_EVENTS_GOLD_CONFIG
    )
    
    # ===== CREATE ENRICHED ANALYTICS TABLE =====
    print("\n" + "-"*70)
    print("🔗 CREATING ENRICHED ANALYTICS TABLE")
    print("-"*70)
    
    # Merge athlete data with NOC regions
    print("\n📊 Merging athlete events with regional data...")
    enriched = silver_athletes.merge(
        silver_noc[["noc", "region"]], 
        left_on="noc", 
        right_on="noc", 
        how="left"
    )
    enriched["_layer"] = "gold"
    enriched["_created_timestamp"] = pd.Timestamp.now()
    print(f"✓ Enriched table created: {len(enriched)} rows")
    
    # Create medal winners report
    print("\n🏆 Creating medal winners report...")
    medal_winners = enriched[enriched["medal"].notna()].copy()
    medal_by_country = medal_winners.groupby("region", as_index=False).agg({
        "medal": "count",
        "id": "count"
    }).rename(columns={"medal": "medal_count", "id": "athlete_count"})
    medal_by_country = medal_by_country.sort_values("medal_count", ascending=False)
    
    print("\n🥇 Top 10 Countries by Medal Count:")
    print(medal_by_country.head(10).to_string(index=False))
    
    # Create sport statistics
    print("\n\n⚽ Sport Statistics:")
    sport_stats = medal_winners.groupby("sport", as_index=False).agg({
        "medal": "count",
        "id": "count",
        "age": ["mean", "min", "max"]
    }).rename(columns={"medal": "medal_count", "id": "athlete_count"})
    print(sport_stats.to_string(index=False))
    
    # Save enriched analytics
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    enriched_path = GOLD_DIR / f"enriched_medal_analytics_{timestamp}.csv"
    enriched.to_csv(enriched_path, index=False)
    print(f"\n💾 Saved enriched data to: {enriched_path}")
    
    # Save medal report
    medal_report_path = GOLD_DIR / f"medal_by_country_{timestamp}.csv"
    medal_by_country.to_csv(medal_report_path, index=False)
    print(f"💾 Saved medal report to: {medal_report_path}")
    
    print("\n" + "="*70)
    print("✅ GOLD LAYER COMPLETED")
    print("="*70)
    print(f"\nGold files saved to: {GOLD_DIR}")
    
    return gold_athletes, enriched, medal_by_country


# ===== COMPLETE PIPELINE =====
def run_complete_pipeline():
    """Run the complete ETL pipeline (Bronze -> Silver -> Gold)"""
    print("\n" + "="*70)
    print("🚀 COMPLETE ETL PIPELINE - OLYMPIC GAMES DATA")
    print("="*70)
    
    # Bronze
    bronze_athletes, bronze_noc = run_bronze_layer()
    
    # Silver
    silver_athletes, silver_noc = run_silver_layer(bronze_athletes, bronze_noc)
    
    # Gold
    gold_athletes, enriched, medal_report = run_gold_layer(silver_athletes, silver_noc)
    
    print("\n" + "="*70)
    print("✅ COMPLETE ETL PIPELINE FINISHED SUCCESSFULLY!")
    print("="*70)
    print(f"\n📁 Output locations:")
    print(f"   🔵 Bronze: {BRONZE_DIR}")
    print(f"   ⚪ Silver: {SILVER_DIR}")
    print(f"   🟡 Gold:   {GOLD_DIR}")
    
    return bronze_athletes, bronze_noc, silver_athletes, silver_noc, gold_athletes, enriched


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run ETL Pipeline layers")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Which layer to run (default: all)"
    )
    
    args = parser.parse_args()
    
    if args.layer == "bronze":
        run_bronze_layer()
    elif args.layer == "silver":
        run_silver_layer()
    elif args.layer == "gold":
        run_gold_layer()
    elif args.layer == "all":
        run_complete_pipeline()
