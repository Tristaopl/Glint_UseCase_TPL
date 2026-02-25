"""
Gold Layer - Star Schema Data Warehouse
Creates dimensional and fact tables for business analytics
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional
from config import GOLD_DIR, GOLD_STAGING_DIR, FILE_FORMAT


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

    def save_table(self, df, table_name, target_dir: Optional[Path] = None):
        """Save a table to gold layer or a specified directory"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}.{self.file_format}"
        base_dir = target_dir or self.gold_dir
        filepath = base_dir / filename

        if self.file_format == "parquet":
            df.to_parquet(filepath, index=False)
        elif self.file_format == "csv":
            df.to_csv(filepath, index=False)

        print(f"   💾 Saved to: {filepath}")
        return filepath

    # ===== STAR SCHEMA CREATION =====

    def create_star_schema(self, df_athletes, df_noc_regions,
                           save_tables: bool = True,
                           target_dir: Optional[Path] = None):
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
        if save_tables:
            print("\n💾 SAVING TABLES...")
            print("\nDimension Tables:")
            self.save_table(dim_athlete, "dim_athlete", target_dir=target_dir)
            self.save_table(dim_country, "dim_country", target_dir=target_dir)
            self.save_table(dim_event, "dim_event", target_dir=target_dir)
            self.save_table(dim_games, "dim_games", target_dir=target_dir)
            
            print("\nFact Table:")
            self.save_table(fact, "fact_athlete_event_result", target_dir=target_dir)

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
        

        print("\n" + "="*70)
        print("✅ STAR SCHEMA CREATED SUCCESSFULLY!")
        print("="*70)

        return dim_athlete, dim_country, dim_event, dim_games, fact


class GoldStarBuilder:
    """Builds star schema with an optional staging step"""

    def __init__(self, staging_dir: Optional[Path] = None):
        self.gold = GoldLayer()
        self.staging_dir = staging_dir or GOLD_STAGING_DIR

    def _load_dim_if_exists(self, filename: str):
        dim_path = GOLD_DIR / filename
        if dim_path.exists():
            return pd.read_parquet(dim_path)
        return None

    def _next_key(self, df: pd.DataFrame, key_col: str) -> int:
        if df is None or df.empty:
            return 1
        return int(df[key_col].max()) + 1

    def _merge_dim_athlete_scd1(self, existing_dim: pd.DataFrame,
                               staged_dim: pd.DataFrame) -> pd.DataFrame:
        if existing_dim is None or existing_dim.empty:
            return staged_dim

        merged = existing_dim.copy()
        now = datetime.now()

        for _, row in staged_dim.iterrows():
            athlete_id = row["id"]
            mask = merged["id"] == athlete_id
            if mask.any():
                merged.loc[mask, ["name", "sex", "team", "height", "weight"]] = \
                    row[["name", "sex", "team", "height", "weight"]].values
                merged.loc[mask, "dw_update_date"] = now
            else:
                new_key = self._next_key(merged, "athlete_key")
                new_row = {
                    "athlete_key": new_key,
                    "id": row["id"],
                    "name": row["name"],
                    "sex": row["sex"],
                    "team": row["team"],
                    "height": row["height"],
                    "weight": row["weight"],
                    "dw_insert_date": now,
                    "dw_update_date": now
                }
                merged = pd.concat([merged, pd.DataFrame([new_row])], ignore_index=True)

        return merged

    def _merge_dim_country_scd2(self, existing_dim: pd.DataFrame,
                                staged_dim: pd.DataFrame) -> pd.DataFrame:
        if existing_dim is None or existing_dim.empty:
            return staged_dim

        merged = existing_dim.copy()
        now = datetime.now()

        for _, row in staged_dim.iterrows():
            noc = row["noc"]
            current_mask = (merged["noc"] == noc) & (merged["is_current"] == True)
            if not current_mask.any():
                new_key = self._next_key(merged, "country_key")
                new_row = {
                    "country_key": new_key,
                    "noc": row["noc"],
                    "region": row["region"],
                    "notes": row["notes"],
                    "effective_date": now,
                    "end_date": pd.Timestamp("2999-12-31"),
                    "is_current": True,
                    "dw_insert_date": now
                }
                merged = pd.concat([merged, pd.DataFrame([new_row])], ignore_index=True)
                continue

            current_row = merged[current_mask].iloc[0]
            changed = False
            for col in ["region", "notes"]:
                if str(row[col]) != str(current_row[col]):
                    changed = True
                    break

            if changed:
                merged.loc[current_mask, "end_date"] = now
                merged.loc[current_mask, "is_current"] = False
                new_key = self._next_key(merged, "country_key")
                new_row = {
                    "country_key": new_key,
                    "noc": row["noc"],
                    "region": row["region"],
                    "notes": row["notes"],
                    "effective_date": now,
                    "end_date": pd.Timestamp("2999-12-31"),
                    "is_current": True,
                    "dw_insert_date": now
                }
                merged = pd.concat([merged, pd.DataFrame([new_row])], ignore_index=True)

        return merged

    def _merge_dim_type0(self, existing_dim: pd.DataFrame,
                         staged_dim: pd.DataFrame,
                         natural_keys: list,
                         key_col: str) -> pd.DataFrame:
        if existing_dim is None or existing_dim.empty:
            return staged_dim

        merged = existing_dim.copy()
        now = datetime.now()
        existing_keys = set(
            tuple(x) for x in merged[natural_keys].itertuples(index=False, name=None)
        )

        new_rows = []
        for _, row in staged_dim.iterrows():
            nk = tuple(row[natural_keys].values)
            if nk not in existing_keys:
                new_key = self._next_key(merged, key_col)
                new_row = row.to_dict()
                new_row[key_col] = new_key
                new_row["dw_insert_date"] = now
                new_rows.append(new_row)
                existing_keys.add(nk)

        if new_rows:
            merged = pd.concat([merged, pd.DataFrame(new_rows)], ignore_index=True)

        return merged

    def build_star_schema(self, df_athletes: pd.DataFrame,
                          df_noc_regions: pd.DataFrame,
                          run_quality_checks: bool = True):
        """
        Build complete star schema without governance side effects.
        
        Args:
            df_athletes: Silver layer athlete events data
            df_noc_regions: Silver layer NOC regions data
            run_quality_checks: Unused (kept for compatibility)
        """
        print("\n" + "=" * 70)
        print("🟡 GOLD LAYER - STAR SCHEMA BUILD")
        print("=" * 70)

        return self.gold.create_star_schema(
            df_athletes,
            df_noc_regions,
            save_tables=False
        )

    def build_star_schema_incremental(self, df_athletes: pd.DataFrame,
                                      df_noc_regions: pd.DataFrame,
                                      existing_fact_path: Optional[str] = None,
                                      run_quality_checks: bool = True):
        """
        Build star schema with a staging step and incremental fact append.
        
        Args:
            df_athletes: Silver layer athlete events data
            df_noc_regions: Silver layer NOC regions data
            existing_fact_path: Path to existing fact table (None = full rebuild)
            run_quality_checks: Unused (kept for compatibility)
        """
        print("\n" + "=" * 70)
        print("🟡 GOLD LAYER - INCREMENTAL STAR SCHEMA BUILD")
        print("=" * 70)

        # Stage a full build in gold_staging
        print("\n📦 STAGING: Building new dims/facts in gold_staging")
        dim_athlete_stage, dim_country_stage, dim_event_stage, dim_games_stage, staged_fact = \
            self.gold.create_star_schema(
                df_athletes,
                df_noc_regions,
                save_tables=True,
                target_dir=self.staging_dir
            )

        # If no existing fact table, return staged results as full build
        if existing_fact_path is None or not Path(existing_fact_path).exists():
            stats = {
                "mode": "full",
                "new_rows": len(staged_fact),
                "appended_rows": 0,
                "duplicate_keys_skipped": 0
            }
            return dim_athlete_stage, dim_country_stage, dim_event_stage, dim_games_stage, staged_fact, stats

        # Merge staged dims into existing dims
        print("\n🔁 MERGE: Updating dimensions from staging")
        existing_athlete = self._load_dim_if_exists("dim_athlete.parquet")
        existing_country = self._load_dim_if_exists("dim_country.parquet")
        existing_event = self._load_dim_if_exists("dim_event.parquet")
        existing_games = self._load_dim_if_exists("dim_games.parquet")

        dim_athlete = self._merge_dim_athlete_scd1(existing_athlete, dim_athlete_stage)
        dim_country = self._merge_dim_country_scd2(existing_country, dim_country_stage)
        dim_event = self._merge_dim_type0(existing_event, dim_event_stage,
                                          natural_keys=["event", "sport"],
                                          key_col="event_key")
        dim_games = self._merge_dim_type0(existing_games, dim_games_stage,
                                          natural_keys=["games", "year", "season", "city"],
                                          key_col="games_key")

        # Incremental append using merged dimensions
        print("\n♻️  INCREMENTAL MODE: Appending to existing fact table")
        existing_fact = pd.read_parquet(existing_fact_path)
        print(f"📖 Loaded existing fact table: {len(existing_fact)} rows")

        new_fact = self.gold.create_fact_athlete_event_result(
            df_athletes, dim_athlete, dim_country, dim_event, dim_games
        )

        key_columns = ["athlete_key", "event_key", "games_key"]
        existing_keys = (existing_fact[key_columns]
                         .astype(str)
                         .agg("|".join, axis=1))
        staged_keys = (new_fact[key_columns]
                       .astype(str)
                       .agg("|".join, axis=1))

        mask_new = ~staged_keys.isin(existing_keys)
        new_rows_only = new_fact[mask_new].reset_index(drop=True)

        duplicates_skipped = len(new_fact) - len(new_rows_only)
        print(f"✅ New unique records: {len(new_rows_only)}")
        if duplicates_skipped > 0:
            print(f"⏭️  Duplicate keys skipped: {duplicates_skipped}")

        merged_fact = pd.concat([existing_fact, new_rows_only], ignore_index=True)

        print("\n📊 Fact table statistics:")
        print(f"   • Before:  {len(existing_fact)} rows")
        print(f"   • Added:   {len(new_rows_only)} rows")
        print(f"   • After:   {len(merged_fact)} rows")

        stats = {
            "mode": "incremental",
            "new_rows": len(new_fact),
            "appended_rows": len(new_rows_only),
            "duplicate_keys_skipped": duplicates_skipped
        }

        return dim_athlete, dim_country, dim_event, dim_games, merged_fact, stats

