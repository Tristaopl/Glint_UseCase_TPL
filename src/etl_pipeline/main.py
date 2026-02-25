"""
Main ETL Pipeline Orchestrator
Coordinates the flow through Bronze -> Silver -> Gold (Star Schema)
"""
from pathlib import Path

from bronze import BronzeLayer
from silver import SilverLayer
from gold import GoldStarBuilder
from governance.lineage import LineageTracker
from config import GOLD_DIR, INCREMENTAL_MODE, RUN_QUALITY_CHECKS


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
    builder = GoldStarBuilder()
    lineage = LineageTracker()

    try:
        # Bronze: ingest raw CSVs
        lineage.start_run(
            layer="bronze",
            table_name="raw_ingest",
            source=f"{athlete_csv}, {noc_csv}"
        )
        bronze_athletes = bronze.ingest_data(str(athlete_csv), "athlete_events")
        bronze_noc = bronze.ingest_data(str(noc_csv), "noc_regions")
        lineage.log_transformation("ingest_csv", {
            "tables": ["athlete_events", "noc_regions"]
        })
        lineage.end_run(
            status="success",
            row_count_out=len(bronze_athletes) + len(bronze_noc)
        )

        # Silver: clean and standardize
        lineage.start_run(
            layer="silver",
            table_name="standardized",
            source="bronze"
        )
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
        lineage.log_transformation("standardize_columns", {
            "tables": ["athlete_events", "noc_regions"]
        })
        lineage.end_run(
            status="success",
            row_count_in=len(bronze_athletes) + len(bronze_noc),
            row_count_out=len(silver_athletes) + len(silver_noc)
        )

        # Gold: build star schema
        lineage.start_run(
            layer="gold",
            table_name="star_schema",
            source="silver"
        )
        fact_path = GOLD_DIR / "fact_athlete_event_result.parquet"

        if INCREMENTAL_MODE:
            print("\n♻️  INCREMENTAL MODE: Appending to existing fact table")
            dim_a, dim_c, dim_e, dim_g, fact, stats = builder.build_star_schema_incremental(
                silver_athletes,
                silver_noc,
                existing_fact_path=str(fact_path),
                run_quality_checks=RUN_QUALITY_CHECKS
            )
            print(f"   • Mode: {stats['mode']}")
            print(f"   • Appended: {stats['appended_rows']} rows")
        else:
            print("\n🆕 FULL BUILD MODE: Creating complete star schema")
            dim_a, dim_c, dim_e, dim_g, fact = builder.build_star_schema(
                silver_athletes,
                silver_noc,
                run_quality_checks=RUN_QUALITY_CHECKS
            )
            print(f"   • Created: {len(fact)} fact records")

        lineage.log_transformation("build_star_schema", {
            "incremental": INCREMENTAL_MODE,
            "fact_rows": len(fact)
        })
        lineage.end_run(
            status="success",
            row_count_in=len(silver_athletes) + len(silver_noc),
            row_count_out=len(fact)
        )

        # Save gold outputs
        dim_a.to_parquet(GOLD_DIR / "dim_athlete.parquet", index=False)
        dim_c.to_parquet(GOLD_DIR / "dim_country.parquet", index=False)
        dim_e.to_parquet(GOLD_DIR / "dim_event.parquet", index=False)
        dim_g.to_parquet(GOLD_DIR / "dim_games.parquet", index=False)
        fact.to_parquet(fact_path, index=False)

        print("\n📂 Gold outputs saved to:")
        print(f"   • dim_athlete.parquet")
        print(f"   • dim_country.parquet")
        print(f"   • dim_event.parquet")
        print(f"   • dim_games.parquet")
        print(f"   • fact_athlete_event_result.parquet")

        log_file = lineage.save_lineage_log()
        print(f"\n📋 Lineage log saved to: {log_file}")

        print("\n✅ Pipeline completed successfully")
        print(f"   • Mode: {'Incremental' if INCREMENTAL_MODE else 'Full rebuild'}")
        print(f"   • Quality checks: {'Enabled' if RUN_QUALITY_CHECKS else 'Disabled'}")
        return 0
    except Exception as exc:
        lineage.end_run(status="failed", errors=[str(exc)])
        lineage.save_lineage_log()
        raise


if __name__ == "__main__":
    exit_code = main()
    raise SystemExit(exit_code)
