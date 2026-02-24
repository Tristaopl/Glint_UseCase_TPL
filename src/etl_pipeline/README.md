# Pandas ETL Pipeline - Bronze, Silver, Gold Architecture

A three-layer data processing pipeline using pandas for clean, organized data workflows.

## Architecture

### 🔵 Bronze Layer
- **Purpose**: Raw data ingestion
- **Function**: Load data from various sources (CSV, Parquet, JSON, DataFrames, lists)
- **Output**: Minimal transformation, adds ingestion metadata
- **File**: `bronze.py`

### ⚪ Silver Layer
- **Purpose**: Data cleaning and transformation
- **Function**: Remove duplicates, handle missing values, standardize formats, validate types
- **Output**: Clean, standardized data ready for analysis
- **File**: `silver.py`

### 🟡 Gold Layer
- **Purpose**: Business-ready analytics tables
- **Function**: Aggregations, metrics calculation, data enrichment
- **Output**: Structured data for reporting and dashboards
- **File**: `gold.py`

## Project Structure

```
etl_pipeline/
├── data/
│   ├── bronze/          # Raw ingested data
│   ├── silver/          # Cleaned data
│   └── gold/            # Analytics ready data
├── bronze.py            # Bronze layer implementation
├── silver.py            # Silver layer implementation
├── gold.py              # Gold layer implementation
├── main.py              # Pipeline orchestrator
├── config.py            # Configuration settings
└── __init__.py          # Package initialization
```

## Quick Start

### 1. Basic Usage

```python
from etl_pipeline import ETLPipeline

# Initialize pipeline
pipeline = ETLPipeline()

# Define your data
data = {
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["NY", "Paris", "London"]
}

# Run the pipeline
bronze_df, silver_df, gold_df = pipeline.run(data, "employees")
```

### 2. With Configuration

```python
# Silver layer config
silver_config = {
    "type_mapping": {"age": "int", "salary": "float"},
    "remove_outliers": True
}

# Gold layer config
gold_config = {
    "group_by": ["city"],
    "aggregations": {"age": "mean", "salary": "sum"},
    "metrics": {
        "total_employees": lambda df: df.groupby("city").size()
    }
}

# Run with config
bronze_df, silver_df, gold_df = pipeline.run(
    data, 
    "employees",
    silver_config=silver_config,
    gold_config=gold_config
)
```

### 3. Loading from Files

```python
# Load from CSV
bronze_df = pipeline.bronze.ingest_data("data.csv", "sales_data")

# Load from Parquet
bronze_df = pipeline.bronze.ingest_data("data.parquet", "sales_data")
```

## Layer Methods

### Bronze Layer

```python
bronze = BronzeLayer()

# Load raw data (returns DataFrame)
df = bronze.load_raw_data(source_data, "table_name")

# Save to bronze layer
bronze.save_bronze_data(df, "table_name")

# Complete ingestion
df = bronze.ingest_data(source_data, "table_name")
```

### Silver Layer

```python
silver = SilverLayer()

# Transform data
clean_df = silver.clean_and_transform(df, config)

# Individual operations
df = silver.remove_duplicates(df)
df = silver.handle_missing_values(df, strategy="drop")
df = silver.standardize_column_names(df)
df = silver.remove_outliers(df)
```

### Gold Layer

```python
gold = GoldLayer()

# Create analytics table
analytics_df = gold.create_analytics_table(df, "employees", config)

# Individual operations
agg_df = gold.aggregate_by_dimension(df, ["city"])
enriched_df = gold.enrich_data(df, enrichment_source, "key_column")
metrics_df = gold.calculate_metrics(df, metrics_config)
```

## Configuration Options

### Silver Layer Config

```python
silver_config = {
    "type_mapping": {           # Optional: column dtype conversions
        "age": "int",
        "salary": "float"
    },
    "remove_outliers": True,    # Optional: remove statistical outliers
    "transformations": [        # Optional: custom transformation functions
        lambda df: df[df['age'] > 18],  # Age filter
        lambda df: df.rename(columns={"col": "new_col"})
    ]
}
```

### Gold Layer Config

```python
gold_config = {
    "group_by": ["city"],                    # Columns to aggregate
    "aggregations": {                        # Aggregation functions
        "salary": "sum",
        "age": "mean"
    },
    "metrics": {                             # Calculated metrics
        "avg_salary": lambda df: df["salary"].mean(),
        "emp_count": lambda df: len(df)
    },
    "enrichment": {                          # Optional: join additional data
        "data": enrichment_df,
        "left_on": "emp_id",
        "right_on": "id"
    }
}
```

## Data Flow Example

```
Raw CSV
   ↓
[BRONZE] → raw_data_20260224_120000.csv (minimal processing, + metadata)
   ↓
[SILVER] → employees_cleaned_20260224_120001.csv (duplicates removed, nulls handled, standardized)
   ↓
[GOLD] → employees_analytics_20260224_120002.csv (aggregated by city, metrics calculated)
```

## Customization

### Custom Transformations

```python
# Define custom transformation functions
def age_filter(df):
    return df[df['age'] > 18]

def add_age_group(df):
    df['age_group'] = pd.cut(df['age'], bins=[0, 18, 30, 60, 100])
    return df

# Use in pipeline
silver_config = {
    "transformations": [age_filter, add_age_group]
}
```

### Custom Metrics

```python
# Define custom metrics
metrics_config = {
    "salary_variance": lambda df: df["salary"].var(),
    "salary_percentile_90": lambda df: df["salary"].quantile(0.9),
    "age_std": lambda df: df["age"].std()
}

# Use in gold layer
gold_config = {
    "metrics": metrics_config
}
```

## Dependencies

- `pandas>=1.3.0`
- `numpy>=1.21.0`

Install with:
```bash
pip install -r requirements.txt
```

## File Formats Supported

- CSV (.csv)
- Parquet (.parquet)
- JSON (.json)

Change default format in `config.py`:
```python
FILE_FORMAT = "parquet"  # or "csv", "json"
```

## Best Practices

1. **Keep transformations atomic** - Each cleaning/transformation function should do one thing
2. **Use configurations** - Don't hardcode transformations, use config dictionaries
3. **Validate data quality** - Check for outliers and missing values at silver stage
4. **Version your gold tables** - Add version numbers for tracking changes
5. **Document custom transformations** - Make transformations clear and reusable

## Logging and Debugging

Each layer prints progress indicators:
- 📥 Ingestion events
- 🧹 Cleaning operations
- 🏆 Gold layer transformations
- 💾 File saves
- ⚠️ Warnings

## Future Enhancements

- [ ] Data quality metrics and validation framework
- [ ] Lineage tracking for data transformations
- [ ] Incremental loading support
- [ ] Data profiling and statistical summaries
- [ ] Automated schema detection
- [ ] Integration with scheduling tools (Airflow, Prefect)
- [ ] Performance optimization for large datasets
