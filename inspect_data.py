import pandas as pd

# Inspect first CSV
print("=" * 70)
print("athlete_events.csv")
print("=" * 70)
df1 = pd.read_csv('data_source/athlete_events.csv')
print(f"\nShape: {df1.shape}")
print(f"\nColumns: {df1.columns.tolist()}")
print(f"\nFirst 3 rows:")
print(df1.head(3))
print(f"\nData types:")
print(df1.dtypes)
print(f"\nMissing values:")
print(df1.isnull().sum())

# Inspect second CSV
print("\n" + "=" * 70)
print("noc_regions.csv")
print("=" * 70)
df2 = pd.read_csv('data_source/noc_regions.csv')
print(f"\nShape: {df2.shape}")
print(f"\nColumns: {df2.columns.tolist()}")
print(f"\nFirst 3 rows:")
print(df2.head(3))
print(f"\nData types:")
print(df2.dtypes)
print(f"\nMissing values:")
print(df2.isnull().sum())
