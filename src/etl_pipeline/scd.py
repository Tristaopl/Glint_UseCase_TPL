"""
Slowly Changing Dimensions (SCD) Implementation
SCD Type 1 and Type 2 operations
"""
import pandas as pd
from datetime import datetime


class SCD:
    """Slowly Changing Dimension operations"""

    @staticmethod
    def scd1_upsert(existing_dim: pd.DataFrame, new_data: pd.DataFrame,
                    key_column: str) -> pd.DataFrame:
        """
        SCD Type 1: Overwrite old values with new ones
        
        Args:
            existing_dim: Current dimension table
            new_data: New/updated dimension data
            key_column: Primary key column name
        
        Returns:
            Updated dimension table with SCD1 changes
        
        Example:
            >>> existing = pd.DataFrame({
            ...     'athlete_id': [1, 2],
            ...     'name': ['Alice', 'Bob'],
            ...     'team': ['USA', 'UK']
            ... })
            >>> new = pd.DataFrame({
            ...     'athlete_id': [1],
            ...     'name': ['Alice Updated'],
            ...     'team': ['USA Updated']
            ... })
            >>> result = SCD.scd1_upsert(existing, new, 'athlete_id')
        """
        print(f"📝 Applying SCD1 (Type 1) to {key_column}...")
        
        # Remove existing records that are being updated
        df_updated = existing_dim[~existing_dim[key_column].isin(new_data[key_column])].copy()
        
        # Add new/updated records
        df_result = pd.concat([df_updated, new_data], ignore_index=True)
        
        rows_updated = len(new_data)
        print(f"   ✓ {rows_updated} rows updated/inserted")
        
        return df_result

    @staticmethod
    def scd2_upsert(existing_dim: pd.DataFrame, new_data: pd.DataFrame,
                    key_column: str, change_columns: list,
                    effective_date_col: str = 'effective_date',
                    end_date_col: str = 'end_date',
                    is_current_col: str = 'is_current') -> pd.DataFrame:
        """
        SCD Type 2: Track historical changes with effective dates
        
        Creates new records for changed dimensions while maintaining history
        of previous versions
        
        Args:
            existing_dim: Current dimension table (must have scd2 columns)
            new_data: New/updated dimension data (must have key_column)
            key_column: Primary key column name
            change_columns: List of columns to track for changes
            effective_date_col: Name of effective_date column
            end_date_col: Name of end_date column
            is_current_col: Name of is_current flag column
        
        Returns:
            Updated dimension table with SCD2 changes
        
        Example:
            >>> existing = pd.DataFrame({
            ...     'noc': ['USA', 'CHN'],
            ...     'region': ['United States', 'China'],
            ...     'effective_date': [pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-01')],
            ...     'end_date': [pd.Timestamp('2999-12-31'), pd.Timestamp('2999-12-31')],
            ...     'is_current': [True, True]
            ... })
            >>> new = pd.DataFrame({
            ...     'noc': ['CHN'],
            ...     'region': ['People''s Republic of China']
            ... })
            >>> result = SCD.scd2_upsert(existing, new, 'noc', ['region'])
        """
        print(f"📝 Applying SCD2 (Type 2) to {key_column}...")
        
        if effective_date_col not in existing_dim.columns:
            raise ValueError(f"Column '{effective_date_col}' not found in dimension table")
        
        df_result = existing_dim.copy()
        now = datetime.now()
        
        # Find changed records
        changed_records = []
        
        for _, new_record in new_data.iterrows():
            key_value = new_record[key_column]
            
            # Find existing record(s) for this key
            existing_records = df_result[
                (df_result[key_column] == key_value) & 
                (df_result[is_current_col] == True)
            ]
            
            if len(existing_records) == 0:
                # New record - insert as current
                new_record_dict = new_record.to_dict()
                new_record_dict[effective_date_col] = now
                new_record_dict[end_date_col] = pd.Timestamp('2999-12-31')
                new_record_dict[is_current_col] = True
                changed_records.append(new_record_dict)
                print(f"   ✓ New record: {key_column}={key_value}")
                
            else:
                # Existing record - check if changed
                existing_record = existing_records.iloc[0]
                
                # Check if any tracked column changed
                has_changes = False
                for col in change_columns:
                    if col in new_record.index and col in existing_record.index:
                        if str(new_record[col]) != str(existing_record[col]):
                            has_changes = True
                            break
                
                if has_changes:
                    # Close old record
                    df_result.loc[existing_record.name, end_date_col] = now
                    df_result.loc[existing_record.name, is_current_col] = False
                    
                    # Insert new version
                    new_record_dict = new_record.to_dict()
                    new_record_dict[effective_date_col] = now
                    new_record_dict[end_date_col] = pd.Timestamp('2999-12-31')
                    new_record_dict[is_current_col] = True
                    changed_records.append(new_record_dict)
                    print(f"   ✓ Updated record: {key_column}={key_value}")
        
        # Add changed records to result
        if changed_records:
            df_changed = pd.DataFrame(changed_records)
            df_result = pd.concat([df_result, df_changed], ignore_index=True)
        
        print(f"   ✓ {len(changed_records)} changes tracked")
        return df_result

    @staticmethod
    def get_current_records(dim_table: pd.DataFrame,
                           is_current_col: str = 'is_current') -> pd.DataFrame:
        """
        Get only current (active) records from SCD2 dimension
        
        Args:
            dim_table: Dimension table with SCD2 structure
            is_current_col: Name of is_current flag column
        
        Returns:
            DataFrame with only current records
        """
        return dim_table[dim_table[is_current_col] == True].copy()

    @staticmethod
    def get_history(dim_table: pd.DataFrame, key_value,
                    key_column: str,
                    effective_date_col: str = 'effective_date',
                    end_date_col: str = 'end_date') -> pd.DataFrame:
        """
        Get full history (all versions) of a dimension record
        
        Args:
            dim_table: Dimension table with SCD2 structure
            key_value: Value of the key to retrieve history for
            key_column: Primary key column name
            effective_date_col: Name of effective_date column
            end_date_col: Name of end_date column
        
        Returns:
            DataFrame with all versions of the record
        """
        history = dim_table[dim_table[key_column] == key_value].sort_values(
            effective_date_col
        )
        return history

    @staticmethod
    def validate_scd2_structure(dim_table: pd.DataFrame,
                               key_column: str,
                               effective_date_col: str = 'effective_date',
                               end_date_col: str = 'end_date',
                               is_current_col: str = 'is_current') -> bool:
        """
        Validate that dimension table has proper SCD2 structure
        
        Args:
            dim_table: Dimension table to validate
            key_column: Primary key column name
            effective_date_col: Name of effective_date column
            end_date_col: Name of end_date column
            is_current_col: Name of is_current flag column
        
        Returns:
            True if valid SCD2 structure, False otherwise
        """
        required_cols = [key_column, effective_date_col, end_date_col, is_current_col]
        
        for col in required_cols:
            if col not in dim_table.columns:
                print(f"❌ SCD2 validation failed: Missing column '{col}'")
                return False
        
        # Check that current records have end_date = '2999-12-31'
        current = dim_table[dim_table[is_current_col] == True]
        invalid_end_dates = current[current[end_date_col] != pd.Timestamp('2999-12-31')]
        
        if len(invalid_end_dates) > 0:
            print(f"❌ SCD2 validation failed: {len(invalid_end_dates)} current records have invalid end_date")
            return False
        
        print(f"✅ SCD2 structure valid")
        return True
