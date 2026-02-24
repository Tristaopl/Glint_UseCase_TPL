"""
Data Quality Checks
Simple validation rules for data integrity
"""
import pandas as pd


class QualityChecker:
    """Performs data quality checks"""

    def __init__(self):
        self.checks_performed = []
        self.failures = []

    def check_row_count(self, df: pd.DataFrame, min_rows: int = 0, 
                       table_name: str = "DataFrame"):
        """
        Check if row count is above minimum threshold
        
        Args:
            df: DataFrame to check
            min_rows: Minimum acceptable row count (default: 0)
            table_name: Name of the table for reporting
        """
        row_count = len(df)
        passed = row_count > min_rows
        
        check = {
            "check": "row_count",
            "table": table_name,
            "threshold": min_rows,
            "actual": row_count,
            "passed": passed
        }
        self.checks_performed.append(check)
        
        if not passed:
            message = f"⚠️  {table_name}: Row count {row_count} <= {min_rows}"
            self.failures.append(message)
            print(message)
        else:
            print(f"✅ {table_name}: Row count {row_count} > {min_rows}")
        
        return passed

    def check_null_percentage(self, df: pd.DataFrame, max_null_pct: float = 0.2,
                             table_name: str = "DataFrame"):
        """
        Check if null percentage is below threshold for all columns
        
        Args:
            df: DataFrame to check
            max_null_pct: Maximum acceptable null percentage (default: 0.2 = 20%)
            table_name: Name of the table for reporting
        """
        null_pct = df.isnull().sum(axis=0) / len(df)
        violating_cols = null_pct[null_pct > max_null_pct]
        
        passed = len(violating_cols) == 0
        
        check = {
            "check": "null_percentage",
            "table": table_name,
            "max_threshold": max_null_pct,
            "violating_columns": violating_cols.to_dict() if len(violating_cols) > 0 else {},
            "passed": passed
        }
        self.checks_performed.append(check)
        
        if not passed:
            for col, pct in violating_cols.items():
                message = f"⚠️  {table_name}.{col}: Null % {pct:.1%} > {max_null_pct:.1%}"
                self.failures.append(message)
                print(message)
        else:
            print(f"✅ {table_name}: All columns null % <= {max_null_pct:.1%}")
        
        return passed

    def check_duplicate_keys(self, df: pd.DataFrame, key_columns: list,
                            table_name: str = "DataFrame"):
        """
        Check for duplicate keys
        
        Args:
            df: DataFrame to check
            key_columns: Column(s) that should be unique
            table_name: Name of the table for reporting
        """
        duplicates = df.duplicated(subset=key_columns, keep=False).sum()
        passed = duplicates == 0
        
        check = {
            "check": "duplicate_keys",
            "table": table_name,
            "key_columns": key_columns,
            "duplicate_count": duplicates,
            "passed": passed
        }
        self.checks_performed.append(check)
        
        if not passed:
            message = f"⚠️  {table_name}: {duplicates} duplicate key combinations found"
            self.failures.append(message)
            print(message)
        else:
            print(f"✅ {table_name}: No duplicate keys in {key_columns}")
        
        return passed

    def check_referential_integrity(self, fact_df: pd.DataFrame, dim_df: pd.DataFrame,
                                   fact_key: str, dim_key: str,
                                   fact_table: str = "fact", dim_table: str = "dimension"):
        """
        Check if all foreign keys exist in dimension table
        
        Args:
            fact_df: Fact table DataFrame
            dim_df: Dimension table DataFrame
            fact_key: Column name in fact table
            dim_key: Column name in dimension table
            fact_table: Name of fact table
            dim_table: Name of dimension table
        """
        missing_keys = fact_df[~fact_df[fact_key].isin(dim_df[dim_key])][fact_key].nunique()
        passed = missing_keys == 0
        
        check = {
            "check": "referential_integrity",
            "fact_table": fact_table,
            "dim_table": dim_table,
            "fact_key": fact_key,
            "dim_key": dim_key,
            "missing_keys": missing_keys,
            "passed": passed
        }
        self.checks_performed.append(check)
        
        if not passed:
            message = f"⚠️  {fact_table}.{fact_key} → {dim_table}.{dim_key}: {missing_keys} missing keys"
            self.failures.append(message)
            print(message)
        else:
            print(f"✅ {fact_table} → {dim_table}: Referential integrity OK")
        
        return passed

    def check_data_type(self, df: pd.DataFrame, column: str, expected_dtype,
                       table_name: str = "DataFrame"):
        """
        Check if column has expected data type
        
        Args:
            df: DataFrame to check
            column: Column to check
            expected_dtype: Expected data type
            table_name: Name of the table for reporting
        """
        if column not in df.columns:
            passed = False
            message = f"⚠️  {table_name}: Column '{column}' not found"
            self.failures.append(message)
            print(message)
            return False
        
        actual_dtype = str(df[column].dtype)
        passed = actual_dtype == str(expected_dtype)
        
        check = {
            "check": "data_type",
            "table": table_name,
            "column": column,
            "expected": str(expected_dtype),
            "actual": actual_dtype,
            "passed": passed
        }
        self.checks_performed.append(check)
        
        if not passed:
            message = f"⚠️  {table_name}.{column}: dtype {actual_dtype} != {expected_dtype}"
            self.failures.append(message)
            print(message)
        else:
            print(f"✅ {table_name}.{column}: dtype {actual_dtype} OK")
        
        return passed

    def get_quality_report(self):
        """Get detailed quality check report"""
        total_checks = len(self.checks_performed)
        passed_checks = sum(1 for c in self.checks_performed if c["passed"])
        failed_checks = len(self.failures)
        
        report = {
            "total_checks": total_checks,
            "passed": passed_checks,
            "failed": failed_checks,
            "pass_rate": passed_checks / total_checks if total_checks > 0 else 0,
            "checks": self.checks_performed,
            "failures": self.failures
        }
        
        return report

    def print_quality_summary(self):
        """Print quality check summary"""
        report = self.get_quality_report()
        
        print("\n" + "="*70)
        print("✅ DATA QUALITY REPORT")
        print("="*70)
        print(f"\nTotal Checks:     {report['total_checks']}")
        print(f"Passed:           {report['passed']}")
        print(f"Failed:           {report['failed']}")
        print(f"Pass Rate:        {report['pass_rate']:.1%}")
        
        if report["failures"]:
            print(f"\n⚠️  FAILURES ({len(report['failures'])}):")
            for failure in report["failures"]:
                print(f"   {failure}")
        else:
            print(f"\n✅ All quality checks passed!")
        
        return report

    def assert_quality(self, min_pass_rate: float = 0.8):
        """
        Assert that quality checks meet minimum pass rate
        
        Args:
            min_pass_rate: Minimum acceptable pass rate (default: 0.8 = 80%)
        
        Returns:
            True if quality acceptable, False otherwise
        """
        report = self.get_quality_report()
        
        if report["pass_rate"] >= min_pass_rate:
            print(f"\n✅ Quality acceptable: {report['pass_rate']:.1%} >= {min_pass_rate:.1%}")
            return True
        else:
            print(f"\n❌ Quality unacceptable: {report['pass_rate']:.1%} < {min_pass_rate:.1%}")
            return False
