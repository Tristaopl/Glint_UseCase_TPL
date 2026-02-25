# Olympic ETL Pipeline

Three-layer medallion architecture (Bronze → Silver → Gold) with star schema, SCD support, and governance tracking.

## Features

- **Bronze**: Raw data ingestion with metadata tracking
- **Silver**: Data cleaning, standardization, type validation
- **Gold**: Star schema (4 dimensions + 1 fact table)
- **SCD**: Type 0 (immutable), Type 1 (update), Type 2 (history tracking)
- **Incremental Mode**: Staging + merge logic for delta processing
- **Governance**: Lineage logs (JSON) + quality reports (CSV)

## Project Structure

```
etl_pipeline/
├── data/
│   ├── bronze/          # Raw ingested data
│   ├── silver/          # Cleaned data
│   ├── gold/            # Star schema tables
│   └── gold_staging/    # Incremental staging area
├── logs/                # Lineage tracking (JSON)
├── bronze.py            # Data ingestion
├── silver.py            # Data cleaning
├── gold.py              # Star schema + SCD logic
├── main.py              # Pipeline orchestrator
├── config.py            # Configuration
└── governance/
    ├── quality.py       # Quality checks
    └── lineage.py       # Lineage tracking
```

**Test Files** (workspace root):
- `test_bronze.py`, `test_silver.py`, `test_gold.py`
- `test_incremental_append.py`
- `test_quality.py`

## Quick Start

**Run the pipeline:**
```powershell
cd c:\Users\Trist\OneDrive\Ambiente de Trabalho\LR\Glint_UseCase_TPL
python src/etl_pipeline/main.py
```

**Output:**
- Gold tables: `dim_athlete`, `dim_country`, `dim_event`, `dim_games`, `fact_athlete_event_result`
- Lineage log: `src/etl_pipeline/logs/lineage_*.json`

**Run quality checks:**
```powershell
python test_quality.py
```

**Output:**
- Quality report: `src/etl_pipeline/data/gold/quality_report.csv`

## Configuration (`config.py`)

```python
# Execution Mode
INCREMENTAL_MODE = True  # False = full rebuild, True = incremental

# Directories
GOLD_DIR = "data/gold"
GOLD_STAGING_DIR = "data/gold_staging"
```

## Star Schema

**Dimensions:**
- `dim_athlete` (SCD1): Update-in-place, no history
- `dim_country` (SCD2): Historical tracking with `effective_date`, `end_date`, `is_current`
- `dim_event` (Type0): Immutable
- `dim_games` (Type0): Immutable

**Fact Table:**
- `fact_athlete_event_result`: Foreign keys to all 4 dimensions + measures (age, height, weight, medal_flag)

## Incremental Mode

**Full Rebuild (`INCREMENTAL_MODE = False`):**
- Builds star schema from scratch
- Directly saves to `gold/`
- Overwrites existing tables

**Incremental (`INCREMENTAL_MODE = True`):**
1. Build new dimensions/facts → save to `gold_staging/`
2. Load existing dimensions from `gold/`
3. Merge dimensions per SCD type:
   - **SCD1 (athlete)**: Update in-place, insert new
   - **SCD2 (country)**: Detect changes, version rows
   - **Type0 (event/games)**: Append new only
4. Rebuild fact table with merged dimensions
5. Append new fact records to `gold/`

## Testing

```powershell
python test_bronze.py      # Bronze layer
python test_silver.py      # Silver layer
python test_gold.py        # Star schema
python test_incremental_append.py  # Incremental mode
python test_quality.py     # Quality validation
```

## Data Flow

```
athlete_events.csv + noc_regions.csv
           ↓
[BRONZE]   → Ingest + metadata
           ↓
[SILVER]   → Clean + standardize
           ↓
[GOLD]     → Build star schema
           ↓
gold_staging/ → Merge → gold/
           ↓
[LINEAGE]  → logs/lineage_*.json
[QUALITY]  → quality_report.csv
```

## Dependencies

```bash
pip install pandas numpy pyarrow```