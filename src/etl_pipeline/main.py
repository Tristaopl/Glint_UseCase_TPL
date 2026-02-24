"""
Main ETL Pipeline Orchestrator
Coordinates the flow through Bronze -> Silver -> Gold (Star Schema)
"""
from pathlib import Path

from bronze import BronzeLayer
from silver import SilverLayer
from gold_star import GoldStarBuilder
from governance.lineage import LineageTracker
from governance.quality import QualityChecker
from config import GOLD_DIR


def main() -> int:
    """Run the full ETL pipeline and write gold star schema outputs."""
    
    # Input paths
    athlete_csv = Path("data_source") / "athlete_events.csv"
    noc_csv = Path("data_source") / "noc_regions.csv"
    
    if not athlete_csv.exists():
        print(f"❌ Missing: {athlete_csv}")
        return 1
    if not noc_csv.exists():
        print(f"❌ Missing: {noc_csv}")
        return 1

    print("=" * 70)
    print("🚀 Starting ETL Pipeline (Bronze → Silver → Gold Star)")
    print("=" * 70)

    # Initialize components
    bronze = BronzeLayer()
    silver = SilverLayer()
    lineage = LineageTracker()
    quality = QualityChecker()
    builder = GoldStarBuilder(lineage, quality)

    # Bronze: ingest raw CSVs
    bronze_athletes = bronze.ingest_data(str(athlete_csv), "athlete_events")
    bronze_noc = bronze.ingest_data(str(noc_csv), "noc_regions")

    # Silver: clean and standardize
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

    # Gold: build star schema (full rebuild)
    fact_path = GOLD_DIR / "fact_athlete_event_result.parquet"
    
    dim_a, dim_c, dim_e, dim_g, fact = builder.build_star_schema(
        silver_athletes,
        silver_noc,
        run_quality_checks=True
    )
    print("\n✅ Full build complete")

    # Save gold outputs
    dim_a.to_parquet(GOLD_DIR / "dim_athlete.parquet", index=False)
    dim_c.to_parquet(GOLD_DIR / "dim_country.parquet", index=False)
    dim_e.to_parquet(GOLD_DIR / "dim_event.parquet", index=False)
    dim_g.to_parquet(GOLD_DIR / "dim_games.parquet", index=False)
    fact.to_parquet(fact_path, index=False)

    # Save lineage
    lineage.save_lineage_log()

    print("\n📂 Gold outputs saved to:")
    print(f"   • dim_athlete.parquet")
    print(f"   • dim_country.parquet")
    print(f"   • dim_event.parquet")
    print(f"   • dim_games.parquet")
    print(f"   • fact_athlete_event_result.parquet")

    print("\n✅ Pipeline completed successfully")
    return 0


if __name__ == "__main__":
    exit_code = main()
    raise SystemExit(exit_code)
