"""
Gold Layer - Star Schema Data Warehouse
Creates dimensional and fact tables for business analytics
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
from config import GOLD_DIR, FILE_FORMAT


class GoldLayer:
    """Creates star schema dimensional tables for analytics"""

    def __init__(self):
        self.gold_dir = GOLD_DIR
        self.file_format = FILE_FORMAT

    # ===== DIMENSION TABLES =====

    def create_dim_athlete(self, df_athletes):
        """
        Create Athlete Dimension Table (SCD1)
        
        SCD1: Overwrites old values (no history tracking)
        Since athletes' physical attributes can change, we keep latest data
        """
        print("\n📊 Creating dim_athlete...")
        
        dim_athlete = df_athletes.drop_duplicates(subset=['id'], keep='last')[
            ['id', 'name', 'sex', 'team', 'height', 'weight']
        ].dropna(subset=['id']).copy()
        
        # Add dimension surrogate key
        dim_athlete['athlete_key'] = range(1, len(dim_athlete) + 1)
        dim_athlete['dw_insert_date'] = datetime.now()
        dim_athlete['dw_update_date'] = datetime.now()
        
        print(f"   ✓ {len(dim_athlete)} unique athletes")
        return dim_athlete[['athlete_key', 'id', 'name', 'sex', 'team', 'height', 'weight', 
                            'dw_insert_date', 'dw_update_date']]

    def create_dim_country(self, df_noc):
        """
        Create Country/Region Dimension Table (SCD2)
        
        SCD2: Tracks historical changes with effective dates
        """
        print("\n📊 Creating dim_country...")
        
        dim_country = df_noc.drop_duplicates(subset=['noc'], keep='last')[
            ['noc', 'region', 'notes']
        ].dropna(subset=['noc']).copy()
        
        # Add dimension surrogate key
        dim_country['country_key'] = range(1, len(dim_country) + 1)
        dim_country['effective_date'] = datetime.now()
        dim_country['end_date'] = pd.Timestamp('2999-12-31')
        dim_country['is_current'] = True
        dim_country['dw_insert_date'] = datetime.now()
        
        print(f"   ✓ {len(dim_country)} countries/regions")
        return dim_country[['country_key', 'noc', 'region', 'notes', 
                           'effective_date', 'end_date', 'is_current', 'dw_insert_date']]

    def create_dim_event(self, df_athletes):
        """
        Create Event Dimension Table (Type 0 - No changes)
        
        Type 0: Static, immutable dimensions
        """
        print("\n📊 Creating dim_event...")
        
        dim_event = df_athletes[['event', 'sport']].drop_duplicates().copy()
        
        # Add dimension surrogate key
        dim_event['event_key'] = range(1, len(dim_event) + 1)
        dim_event['dw_insert_date'] = datetime.now()
        
        print(f"   ✓ {len(dim_event)} unique events/sports")
        return dim_event[['event_key', 'event', 'sport', 'dw_insert_date']]

    def create_dim_games(self, df_athletes):
        """
        Create Games Dimension Table (Type 0 - No changes)
        
        Type 0: Static Olympic games information
        """
        print("\n📊 Creating dim_games...")
        
        dim_games = df_athletes[['games', 'year', 'season', 'city']].drop_duplicates().copy()
        
        # Add dimension surrogate key
        dim_games['games_key'] = range(1, len(dim_games) + 1)
        dim_games['dw_insert_date'] = datetime.now()
        
        print(f"   ✓ {len(dim_games)} unique Olympic games")
        return dim_games[['games_key', 'games', 'year', 'season', 'city', 'dw_insert_date']]

    # ===== FACT TABLE =====

    def create_fact_athlete_event_result(self, df_athletes, dim_athlete, dim_country, 
                                         dim_event, dim_games):
        """
        Create Fact Table - Athlete Event Results
        
        Contains metrics and foreign keys to dimension tables
        """
        print("\n📊 Creating fact_athlete_event_result...")
        
        # Start with base data
        fact = df_athletes[['id', 'noc', 'event', 'sport', 'games', 'year', 
                           'season', 'city', 'age', 'height', 'weight', 'medal']].copy()
        
        # Join with dimension tables to get surrogate keys
        fact = fact.merge(
            dim_athlete[['id', 'athlete_key']],
            on='id',
            how='left'
        )
        
        fact = fact.merge(
            dim_country[['noc', 'country_key']],
            on='noc',
            how='left'
        )
        
        fact = fact.merge(
            dim_event[['event', 'sport', 'event_key']],
            on=['event', 'sport'],
            how='left'
        )
        
        fact = fact.merge(
            dim_games[['games', 'year', 'season', 'city', 'games_key']],
            on=['games', 'year', 'season', 'city'],
            how='left'
        )
        
        # Add fact table metrics and metadata
        fact['result_key'] = range(1, len(fact) + 1)
        fact['medal_flag'] = (~fact['medal'].isna()).astype(int)
        fact['dw_insert_date'] = datetime.now()
        fact['dw_update_date'] = datetime.now()
        
        # Select final fact table columns
        fact = fact[[
            'result_key', 'athlete_key', 'country_key', 'event_key', 'games_key',
            'age', 'height', 'weight', 'medal', 'medal_flag',
            'dw_insert_date', 'dw_update_date'
        ]]
        
        fact = fact.dropna(subset=['athlete_key', 'country_key', 'event_key', 'games_key'])
        
        print(f"   ✓ {len(fact)} fact records created")
        return fact

    # ===== SAVE TABLES =====

    def save_table(self, df, table_name):
        """Save a table to gold layer"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}.{self.file_format}"
        filepath = self.gold_dir / filename

        if self.file_format == "parquet":
            df.to_parquet(filepath, index=False)
        elif self.file_format == "csv":
            df.to_csv(filepath, index=False)

        print(f"   💾 Saved to: {filepath}")
        return filepath

    # ===== STAR SCHEMA CREATION =====

    def create_star_schema(self, df_athletes, df_noc_regions):
        """
        Create complete star schema (dimensions + fact)
        
        Args:
            df_athletes: Cleaned athlete events data from silver layer
            df_noc_regions: Cleaned NOC regions data from silver layer
        """
        print("\n" + "="*70)
        print("🟡 GOLD LAYER - CREATING STAR SCHEMA")
        print("="*70)

        # Create dimensions
        print("\n📐 CREATING DIMENSIONS...")
        dim_athlete = self.create_dim_athlete(df_athletes)
        dim_country = self.create_dim_country(df_noc_regions)
        dim_event = self.create_dim_event(df_athletes)
        dim_games = self.create_dim_games(df_athletes)

        # Create fact table
        print("\n📊 CREATING FACT TABLE...")
        fact = self.create_fact_athlete_event_result(
            df_athletes, dim_athlete, dim_country, dim_event, dim_games
        )

        # Save all tables
        print("\n💾 SAVING TABLES...")
        print("\nDimension Tables:")
        self.save_table(dim_athlete, "dim_athlete")
        self.save_table(dim_country, "dim_country")
        self.save_table(dim_event, "dim_event")
        self.save_table(dim_games, "dim_games")
        
        print("\nFact Table:")
        self.save_table(fact, "fact_athlete_event_result")

        # Print schema summary
        print("\n" + "-"*70)
        print("📋 STAR SCHEMA SUMMARY")
        print("-"*70)
        print(f"\n📊 Dimensions:")
        print(f"   • dim_athlete:                {len(dim_athlete):>7} rows")
        print(f"   • dim_country:                {len(dim_country):>7} rows")
        print(f"   • dim_event:                  {len(dim_event):>7} rows")
        print(f"   • dim_games:                  {len(dim_games):>7} rows")
        print(f"\n📈 Fact Table:")
        print(f"   • fact_athlete_event_result:  {len(fact):>7} rows")
        
        print(f"\n🔗 Relationships:")
        print(f"   • Fact → dim_athlete:  {fact['athlete_key'].nunique()} athletes")
        print(f"   • Fact → dim_country:  {fact['country_key'].nunique()} countries")
        print(f"   • Fact → dim_event:    {fact['event_key'].nunique()} events")
        print(f"   • Fact → dim_games:    {fact['games_key'].nunique()} games")
        
        print(f"\n✨ Medal Statistics (from Fact):")
        medals_total = fact['medal_flag'].sum()
        print(f"   • Total medal winners:  {medals_total:>7}")
        print(f"   • Medal rate:           {medals_total/len(fact)*100:>6.2f}%")

        print("\n" + "="*70)
        print("✅ STAR SCHEMA CREATED SUCCESSFULLY!")
        print("="*70)

        return dim_athlete, dim_country, dim_event, dim_games, fact


# ===== QUERY EXAMPLES =====

class GoldQueries:
    """Example queries on the star schema"""

    @staticmethod
    def top_athletes_by_medals(fact, dim_athlete):
        """Top athletes by medal count"""
        result = (fact[fact['medal_flag'] == 1]
                  .groupby('athlete_key')
                  .size()
                  .reset_index(name='medal_count')
                  .merge(dim_athlete[['athlete_key', 'name']], on='athlete_key')
                  .sort_values('medal_count', ascending=False)
                  .head(10))
        return result

    @staticmethod
    def medals_by_country(fact, dim_country):
        """Medal count by country"""
        result = (fact[fact['medal_flag'] == 1]
                  .groupby('country_key')
                  .size()
                  .reset_index(name='medal_count')
                  .merge(dim_country[['country_key', 'region']], on='country_key')
                  .sort_values('medal_count', ascending=False)
                  .head(20))
        return result

    @staticmethod
    def medals_by_sport(fact, dim_event):
        """Medal count by sport"""
        result = (fact[fact['medal_flag'] == 1]
                  .groupby('event_key')['medal_flag']
                  .sum()
                  .reset_index(name='medal_count')
                  .merge(dim_event[['event_key', 'sport']], on='event_key')
                  .groupby('sport')['medal_count']
                  .sum()
                  .reset_index(name='medal_count')
                  .sort_values('medal_count', ascending=False))
        return result

    @staticmethod
    def medals_by_games(fact, dim_games):
        """Medal count by Olympic games"""
        result = (fact[fact['medal_flag'] == 1]
                  .groupby('games_key')
                  .size()
                  .reset_index(name='medal_count')
                  .merge(dim_games[['games_key', 'games', 'year', 'city']], on='games_key')
                  .sort_values('year', ascending=False))
        return result
