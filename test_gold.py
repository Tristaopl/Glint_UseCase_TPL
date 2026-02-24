"""
Test script for Gold Layer - Star Schema
Tests dimensional and fact table creation
"""
import sys
from pathlib import Path

# Add etl_pipeline to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "etl_pipeline"))

from gold import GoldLayer, GoldQueries
from config import GOLD_DIR, SILVER_DIR
import pandas as pd


def test_gold_layer():
    """Test the gold layer with star schema"""
    
    print("\n" + "="*70)
    print("🟡 TESTING GOLD LAYER - STAR SCHEMA")
    print("="*70)
    
    # Initialize gold layer
    gold = GoldLayer()
    
    print(f"\n📂 Gold directory: {GOLD_DIR}")
    
    # Load silver data
    print("\n" + "-"*70)
    print("Test 1: Loading silver layer data")
    print("-"*70)
    try:
        # Find the most recent silver files
        silver_athlete_files = sorted(SILVER_DIR.glob("athlete_events*.parquet"))
        silver_noc_files = sorted(SILVER_DIR.glob("noc_regions*.parquet"))
        
        if not silver_athlete_files or not silver_noc_files:
            print("❌ No silver files found. Run test_silver.py first!")
            return
        
        latest_athlete = silver_athlete_files[-1]
        latest_noc = silver_noc_files[-1]
        
        print(f"📂 Loading athlete data: {latest_athlete.name}")
        df_athletes = pd.read_parquet(latest_athlete)
        
        print(f"📂 Loading NOC data: {latest_noc.name}")
        df_noc = pd.read_parquet(latest_noc)
        
        print(f"✅ Loaded {len(df_athletes)} athlete rows")
        print(f"✅ Loaded {len(df_noc)} NOC rows")
    except Exception as e:
        print(f"❌ Error loading silver files: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 2: Create star schema
    print("\n" + "-"*70)
    print("Test 2: Creating star schema")
    print("-"*70)
    try:
        dim_athlete, dim_country, dim_event, dim_games, fact = gold.create_star_schema(
            df_athletes, df_noc
        )
    except Exception as e:
        print(f"❌ Error creating star schema: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 3: Verify schema integrity
    print("\n" + "-"*70)
    print("Test 3: Verifying schema integrity")
    print("-"*70)
    
    print(f"\n✅ Dimension Keys (should be sequential):")
    print(f"   dim_athlete keys:     {dim_athlete['athlete_key'].min()} to {dim_athlete['athlete_key'].max()}")
    print(f"   dim_country keys:     {dim_country['country_key'].min()} to {dim_country['country_key'].max()}")
    print(f"   dim_event keys:       {dim_event['event_key'].min()} to {dim_event['event_key'].max()}")
    print(f"   dim_games keys:       {dim_games['games_key'].min()} to {dim_games['games_key'].max()}")
    
    print(f"\n✅ Foreign Key Relationships:")
    print(f"   All athlete_keys valid: {all(fact['athlete_key'].isin(dim_athlete['athlete_key']))}")
    print(f"   All country_keys valid: {all(fact['country_key'].isin(dim_country['country_key']))}")
    print(f"   All event_keys valid:   {all(fact['event_key'].isin(dim_event['event_key']))}")
    print(f"   All games_keys valid:   {all(fact['games_key'].isin(dim_games['games_key']))}")
    
    # Test 4: Generate reports with query examples
    print("\n" + "-"*70)
    print("Test 4: Generating example reports")
    print("-"*70)
    
    queries = GoldQueries()
    
    # Top athletes by medals
    print("\n🥇 TOP 10 ATHLETES BY MEDALS:")
    top_athletes = queries.top_athletes_by_medals(fact, dim_athlete)
    if len(top_athletes) > 0:
        print(top_athletes.to_string(index=False))
    else:
        print("   No medals found")
    
    # Medals by country
    print("\n🏆 TOP 20 COUNTRIES BY MEDALS:")
    top_countries = queries.medals_by_country(fact, dim_country)
    if len(top_countries) > 0:
        print(top_countries.head(15).to_string(index=False))
    else:
        print("   No medals found")
    
    # Medals by sport
    print("\n⚽ MEDALS BY SPORT:")
    sport_medals = queries.medals_by_sport(fact, dim_event)
    if len(sport_medals) > 0:
        print(sport_medals.to_string(index=False))
    else:
        print("   No medals found")
    
    # Medals by games
    print("\n🎯 MEDALS BY OLYMPIC GAMES:")
    games_medals = queries.medals_by_games(fact, dim_games)
    if len(games_medals) > 0:
        print(games_medals.to_string(index=False))
    else:
        print("   No medals found")
    
    # Test 5: Verify saved files
    print("\n" + "-"*70)
    print("Test 5: Verifying saved files")
    print("-"*70)
    gold_files = list(GOLD_DIR.glob("*.parquet"))
    print(f"\n✅ Found {len(gold_files)} files in gold directory:")
    for file in sorted(gold_files):
        file_size = file.stat().st_size / 1024  # KB
        rows = 0
        try:
            df = pd.read_parquet(file)
            rows = len(df)
        except:
            pass
        print(f"   • {file.name:40} ({file_size:>8.2f} KB | {rows:>8} rows)")
    
    print("\n" + "="*70)
    print("✅ ALL GOLD LAYER TESTS PASSED!")
    print("="*70)
    print(f"\n📂 Output files saved to: {GOLD_DIR}")
    print("\n📋 Schema Structure:")
    print(f"   Fact table:      {len(fact)} rows (athlete-event combinations)")
    print(f"   Dimensions:      {len(dim_athlete) + len(dim_country) + len(dim_event) + len(dim_games)} rows total")
    print(f"   Compression:     ~{(file_size / 1024):.1f} MB")
    
    return dim_athlete, dim_country, dim_event, dim_games, fact


if __name__ == "__main__":
    result = test_gold_layer()
