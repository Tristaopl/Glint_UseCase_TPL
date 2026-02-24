"""
Gold Layer Star Schema Builder
Orchestrates dimension and fact table creation with SCD support
"""
import pandas as pd
from datetime import datetime
from pathlib import Path

from config import GOLD_DIR, FILE_FORMAT
from gold import GoldLayer
from scd import SCD
from governance.lineage import LineageTracker
from governance.quality import QualityChecker


class GoldStarBuilder:
    """Builds and maintains star schema with SCD support"""

    def __init__(self, lineage_tracker: LineageTracker = None,
                 quality_checker: QualityChecker = None):
        """
        Initialize star schema builder
        
        Args:
            lineage_tracker: Optional LineageTracker instance
            quality_checker: Optional QualityChecker instance
        """
        self.gold = GoldLayer()
        self.lineage = lineage_tracker or LineageTracker()
        self.quality = quality_checker or QualityChecker()
        self.scd = SCD()

    def build_star_schema(self, df_athletes: pd.DataFrame, 
                         df_noc_regions: pd.DataFrame,
                         run_quality_checks: bool = True):
        """
        Build complete star schema with quality checks
        
        Args:
            df_athletes: Silver layer athlete events data
            df_noc_regions: Silver layer NOC regions data
            run_quality_checks: Whether to run quality checks
        """
        print("\n" + "="*70)
        print("🟡 GOLD LAYER - STAR SCHEMA BUILD")
        print("="*70)
        
        # Track the run
        run = self.lineage.start_run("gold", "star_schema", 
                                     source="silver_layer")
        
        try:
            # Create dimensions and fact table
            dim_athlete, dim_country, dim_event, dim_games, fact = \
                self.gold.create_star_schema(df_athletes, df_noc_regions)
            
            # Log transformation
            self.lineage.log_transformation("create_dimensions", {
                "dim_athlete_rows": len(dim_athlete),
                "dim_country_rows": len(dim_country),
                "dim_event_rows": len(dim_event),
                "dim_games_rows": len(dim_games),
                "fact_rows": len(fact)
            })
            
            # Run quality checks if requested
            if run_quality_checks:
                self._run_quality_checks(dim_athlete, dim_country, 
                                        dim_event, dim_games, fact)
            
            # End successful run
            self.lineage.end_run(
                status="success",
                row_count_in=len(df_athletes) + len(df_noc_regions),
                row_count_out=len(dim_athlete) + len(dim_country) + 
                             len(dim_event) + len(dim_games) + len(fact)
            )
            
            return dim_athlete, dim_country, dim_event, dim_games, fact
            
        except Exception as e:
            self.lineage.end_run(status="failed", errors=[str(e)])
            raise

    def _run_quality_checks(self, dim_athlete, dim_country, 
                           dim_event, dim_games, fact):
        """Run comprehensive quality checks on star schema"""
        print("\n" + "-"*70)
        print("🔍 RUNNING QUALITY CHECKS")
        print("-"*70)
        
        # Dimension checks
        self.quality.check_row_count(dim_athlete, min_rows=1, 
                                    table_name="dim_athlete")
        self.quality.check_row_count(dim_country, min_rows=1, 
                                    table_name="dim_country")
        self.quality.check_row_count(dim_event, min_rows=1, 
                                    table_name="dim_event")
        self.quality.check_row_count(dim_games, min_rows=1, 
                                    table_name="dim_games")
        
        # Fact table checks
        self.quality.check_row_count(fact, min_rows=1, 
                                    table_name="fact_athlete_event_result")
        
        # Referential integrity checks
        self.quality.check_referential_integrity(
            fact, dim_athlete, 'athlete_key', 'athlete_key',
            fact_table="fact", dim_table="dim_athlete"
        )
        self.quality.check_referential_integrity(
            fact, dim_country, 'country_key', 'country_key',
            fact_table="fact", dim_table="dim_country"
        )
        
        # Print quality summary
        self.quality.print_quality_summary()

    def maintain_scd2_dimension(self, existing_dim_path: str, new_data: pd.DataFrame,
                               key_column: str, change_columns: list):
        """
        Maintain SCD2 dimension table
        
        Args:
            existing_dim_path: Path to existing dimension table (parquet)
            new_data: New/updated dimension records
            key_column: Primary key column
            change_columns: Columns to track for changes
        
        Returns:
            Updated dimension table with SCD2 changes
        """
        print(f"\n📝 Maintaining SCD2 dimension: {existing_dim_path}")
        
        # Load existing dimension
        existing_dim = pd.read_parquet(existing_dim_path)
        
        # Apply SCD2 logic
        updated_dim = self.scd.scd2_upsert(
            existing_dim, new_data, key_column, change_columns
        )
        
        # Validate SCD2 structure
        self.scd.validate_scd2_structure(updated_dim, key_column)
        
        # Save updated dimension
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(existing_dim_path).parent / f"dim_{key_column}_v{timestamp}.parquet"
        updated_dim.to_parquet(output_path, index=False)
        
        print(f"💾 Updated dimension saved to: {output_path}")
        
        return updated_dim

    def print_schema_summary(self, dim_athlete, dim_country, 
                            dim_event, dim_games, fact):
        """Print star schema summary"""
        print("\n" + "="*70)
        print("📊 STAR SCHEMA SUMMARY")
        print("="*70)
        
        print(f"\n📐 Dimensions:")
        print(f"   • dim_athlete:      {len(dim_athlete):>7} records")
        print(f"   • dim_country:      {len(dim_country):>7} records")
        print(f"   • dim_event:        {len(dim_event):>7} records")
        print(f"   • dim_games:        {len(dim_games):>7} records")
        
        print(f"\n📈 Fact Table:")
        print(f"   • fact_athlete_event_result: {len(fact):>7} records")
        
        print(f"\n🔗 Relationships:")
        print(f"   • Fact → dim_athlete:  {fact['athlete_key'].nunique()} athletes")
        print(f"   • Fact → dim_country:  {fact['country_key'].nunique()} countries")
        print(f"   • Fact → dim_event:    {fact['event_key'].nunique()} events")
        print(f"   • Fact → dim_games:    {fact['games_key'].nunique()} games")

    def build_star_schema_incremental(self, df_athletes: pd.DataFrame,
                                     df_noc_regions: pd.DataFrame,
                                     existing_fact_path: str = None,
                                     run_quality_checks: bool = True):
        """
        Build star schema with incremental fact table append
        
        Args:
            df_athletes: Silver layer athlete events data
            df_noc_regions: Silver layer NOC regions data
            existing_fact_path: Path to existing fact table (None = full rebuild)
            run_quality_checks: Whether to run quality checks
        
        Returns:
            Tuple of (dim_athlete, dim_country, dim_event, dim_games, fact, stats)
        """
        print("\n" + "="*70)
        print("🟡 GOLD LAYER - INCREMENTAL STAR SCHEMA BUILD")
        print("="*70)
        
        # Track the run
        run = self.lineage.start_run("gold", "star_schema_incremental",
                                     source="silver_layer")
        
        try:
            # Determine if incremental or full
            is_incremental = (existing_fact_path is not None and 
                            Path(existing_fact_path).exists())
            
            if is_incremental:
                print("\n♻️  INCREMENTAL MODE: Appending new records to existing fact table")
                result = self._append_to_fact_table_incremental(
                    df_athletes, df_noc_regions, existing_fact_path,
                    run_quality_checks
                )
                dim_athlete, dim_country, dim_event, dim_games, fact, stats = result
            else:
                print("\n🆕 FULL BUILD MODE: Creating complete star schema")
                dim_athlete, dim_country, dim_event, dim_games, fact = \
                    self.gold.create_star_schema(df_athletes, df_noc_regions)
                stats = {
                    "mode": "full",
                    "new_rows": len(fact),
                    "appended_rows": 0,
                    "duplicate_keys_skipped": 0
                }
            
            # Log transformation
            self.lineage.log_transformation("incremental_append", {
                "mode": stats["mode"],
                "new_rows": stats["new_rows"],
                "appended_rows": stats["appended_rows"],
                "duplicates_skipped": stats["duplicate_keys_skipped"],
                "total_fact_rows": len(fact)
            })
            
            # Store high-water mark in lineage
            self.lineage.store_fact_table_checkpoint(
                fact_table_size=len(fact),
                new_rows_processed=stats["new_rows"],
                timestamp=datetime.now()
            )
            
            # Run quality checks if requested
            if run_quality_checks:
                self._run_quality_checks(dim_athlete, dim_country,
                                        dim_event, dim_games, fact)
            
            # End successful run
            self.lineage.end_run(
                status="success",
                row_count_in=len(df_athletes) + len(df_noc_regions),
                row_count_out=len(fact)
            )
            
            return dim_athlete, dim_country, dim_event, dim_games, fact, stats
            
        except Exception as e:
            self.lineage.end_run(status="failed", errors=[str(e)])
            raise

    def _append_to_fact_table_incremental(self, df_athletes: pd.DataFrame,
                                         df_noc_regions: pd.DataFrame,
                                         existing_fact_path: str,
                                         run_quality_checks: bool):
        """
        Internal method to append new records to existing fact table
        
        Args:
            df_athletes: New/updated athlete events
            df_noc_regions: Updated NOC regions
            existing_fact_path: Path to existing fact table
            run_quality_checks: Whether to run quality checks
        
        Returns:
            Tuple of (dimensions, new_fact, statistics)
        """
        # Load existing fact table
        existing_fact = pd.read_parquet(existing_fact_path)
        print(f"📖 Loaded existing fact table: {len(existing_fact)} rows")
        
        # Create dimensions (always fresh)
        dim_athlete, dim_country, dim_event, dim_games, _ = \
            self.gold.create_star_schema(df_athletes, df_noc_regions)
        
        # Create new fact records
        new_fact = self.gold.create_fact_athlete_event_result(
            df_athletes, dim_athlete, dim_country, dim_event, dim_games
        )
        print(f"🆕 Created new fact records: {len(new_fact)} rows")
        
        # Identify composite key for deduplication
        key_columns = ['athlete_key', 'event_key', 'games_key']
        
        # Create composite keys for comparison
        existing_keys = (existing_fact[key_columns]
                        .astype(str)
                        .agg('|'.join, axis=1)
                        .set_index(existing_fact.index))
        
        new_keys = (new_fact[key_columns]
                   .astype(str)
                   .agg('|'.join, axis=1)
                   .set_index(new_fact.index))
        
        # Find rows not in existing fact (new rows)
        mask_new = ~new_keys.isin(existing_keys)
        new_rows_only = new_fact[mask_new].reset_index(drop=True)
        
        duplicates_skipped = len(new_fact) - len(new_rows_only)
        print(f"✅ New unique records: {len(new_rows_only)}")
        if duplicates_skipped > 0:
            print(f"⏭️  Duplicate keys skipped: {duplicates_skipped}")
        
        # Append new rows to existing fact
        merged_fact = pd.concat(
            [existing_fact, new_rows_only],
            ignore_index=True
        )
        
        print(f"")
        print(f"📊 Fact table statistics:")
        print(f"   • Before:  {len(existing_fact)} rows")
        print(f"   • Added:   {len(new_rows_only)} rows")
        print(f"   • After:   {len(merged_fact)} rows")
        
        statistics = {
            "mode": "incremental",
            "new_rows": len(new_fact),
            "appended_rows": len(new_rows_only),
            "duplicate_keys_skipped": duplicates_skipped
        }
        
        return dim_athlete, dim_country, dim_event, dim_games, merged_fact, statistics
