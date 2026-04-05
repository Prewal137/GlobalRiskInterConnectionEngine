"""
🧹 Infrastructure Sector Data Cleaner (Phase 3)

Merges all selected infrastructure datasets into ONE unified dataset.

Input:  data/processed/infrastructure/selected_raw/*.csv
Output: data/processed/infrastructure/final_infrastructure.csv

Steps:
    1. Load all selected files
    2. Standardize column names (lowercase, strip, replace spaces)
    3. Identify key columns (state/country, year)
    4. Keep only numeric columns + identifiers
    5. Handle scaling factors
    6. Handle missing values (forward fill + mean)
    7. Merge all dataframes on state + year
    8. Final cleanup and save
"""

import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 3 levels from pipeline/processing/infrastructure/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

DATA_PATH = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "selected_raw")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "final_infrastructure.csv")

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

print("="*70)
print("🧹 INFRASTRUCTURE SECTOR DATA CLEANING")
print("="*70)
print(f"\n📂 Loading files from: {DATA_PATH}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def standardize_column_names(df):
    """
    Standardize column names:
    - Convert to lowercase
    - Strip whitespace
    - Replace spaces with underscores
    - Remove special characters
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with standardized column names
    """
    # Create mapping of old to new column names
    col_mapping = {}
    for col in df.columns:
        new_col = col.lower().strip()
        new_col = new_col.replace(' ', '_')
        new_col = new_col.replace('-', '_')
        # Remove special characters except underscore
        new_col = ''.join(c if c.isalnum() or c == '_' else '' for c in new_col)
        # Remove multiple consecutive underscores
        while '__' in new_col:
            new_col = new_col.replace('__', '_')
        col_mapping[col] = new_col
    
    df = df.rename(columns=col_mapping)
    
    return df


def identify_and_rename_columns(df):
    """
    Identify key columns and rename them consistently.
    
    Priority:
    1. Look for 'state' or 'state_name' → rename to 'state'
    2. If no state, look for 'country' → rename to 'state' (use country as proxy)
    3. Look for 'year' → ensure it's named 'year'
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with renamed columns
    """
    df = df.copy()
    columns_lower = [col.lower() for col in df.columns]
    
    # Find state column
    state_col = None
    for col_pattern in ['state', 'state_name', 'statename']:
        for col in df.columns:
            if col.lower().replace('_', '').replace(' ', '') == col_pattern.replace('_', ''):
                state_col = col
                break
        if state_col:
            break
    
    # If no state found, use country
    if not state_col:
        for col_pattern in ['country']:
            for col in df.columns:
                if col.lower().replace('_', '').replace(' ', '') == col_pattern:
                    state_col = col
                    print(f"      ⚠️  No 'state' column found, using '{col}' as state")
                    break
            if state_col:
                break
    
    # Rename state column
    if state_col and state_col != 'state':
        df = df.rename(columns={state_col: 'state'})
    
    # Find and rename year column
    year_col = None
    for col in df.columns:
        if col.lower() in ['year', 'years']:
            year_col = col
            break
    
    if year_col and year_col != 'year':
        df = df.rename(columns={year_col: 'year'})
    
    return df


def handle_scaling_factors(df):
    """
    Handle scaling factor columns by multiplying actual values.
    
    Pattern: If there's a column like "scaling_factor_1" after a value column,
    multiply the value column by the scaling factor.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with scaling applied
    """
    df = df.copy()
    cols_to_drop = []
    
    # Find scaling factor columns
    scaling_cols = [col for col in df.columns if 'scaling' in col.lower() and 'factor' in col.lower()]
    
    for scale_col in scaling_cols:
        # Try to find the corresponding value column (usually the column before)
        col_idx = list(df.columns).index(scale_col)
        if col_idx > 0:
            value_col = df.columns[col_idx - 1]
            
            # Check if value column is numeric
            if pd.api.types.is_numeric_dtype(df[value_col]):
                try:
                    # Apply scaling
                    scale_values = df[scale_col].unique()
                    if len(scale_values) == 1:
                        scale_val = scale_values[0]
                        if pd.notna(scale_val) and scale_val != 1:
                            df[value_col] = df[value_col] * scale_val
                            print(f"         ✅ Applied scaling ({scale_val}) to '{value_col}'")
                    cols_to_drop.append(scale_col)
                except Exception as e:
                    print(f"         ⚠️  Could not apply scaling for '{value_col}': {e}")
    
    # Drop scaling columns
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
    
    return df


def keep_only_relevant_columns(df):
    """
    Keep only:
    - state (identifier)
    - year (time)
    - numeric columns (signals)
    
    Remove text/categorical columns like "additional_info", "category", etc.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: Filtered DataFrame
    """
    # Columns to always remove (text/descriptive)
    remove_patterns = [
        'additional',
        'category',
        'description',
        'indicator_type',
        'type_of_area',
        'as_on_date',
        'country_or_region'
    ]
    
    cols_to_keep = ['state', 'year']
    
    for col in df.columns:
        if col in ['state', 'year']:
            continue
        
        # Skip if matches removal patterns
        if any(pattern in col.lower() for pattern in remove_patterns):
            continue
        
        # Keep if numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            cols_to_keep.append(col)
    
    return df[cols_to_keep]


def extract_year_from_string(year_series):
    """
    Extract numeric year from string formats like:
    - "Calendar Year (Jan - Dec), 2011"
    - "Fiscal Year 2020-2021"
    - "2011"
    
    Args:
        year_series (pd.Series): Series with year values
        
    Returns:
        pd.Series: Numeric year values
    """
    import re
    
    def extract_single_year(val):
        if pd.isna(val):
            return np.nan
        
        val_str = str(val)
        
        # Try to find 4-digit year pattern
        matches = re.findall(r'\b((?:19|20)\d{2})\b', val_str)
        if matches:
            # Return the last year found (for ranges like 2020-2021, take 2021)
            return int(matches[-1])
        
        # Try direct numeric conversion
        try:
            year = float(val_str)
            if 1900 <= year <= 2100:
                return int(year)
        except:
            pass
        
        return np.nan
    
    return year_series.apply(extract_single_year)


def clean_single_file(filepath):
    """
    Clean a single infrastructure file.
    
    Steps:
    1. Load file
    2. Standardize column names
    3. Identify and rename key columns
    4. Handle scaling factors
    5. Keep only relevant columns
    6. Handle missing values
    
    Args:
        filepath (str): Path to CSV file
        
    Returns:
        pd.DataFrame or None: Cleaned DataFrame
    """
    filename = os.path.basename(filepath)
    
    try:
        # Load file
        df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
        
        if df.empty:
            print(f"   ⏭️  Skipped (empty): {filename}")
            return None
        
        print(f"   📄 Processing: {filename} ({len(df)} rows, {len(df.columns)} cols)")
        
        # Step 1: Standardize column names
        df = standardize_column_names(df)
        
        # Step 2: Identify and rename columns
        df = identify_and_rename_columns(df)
        
        # Check if required columns exist
        if 'state' not in df.columns or 'year' not in df.columns:
            print(f"      ⚠️  Missing required columns (state/year), skipping")
            return None
        
        # Step 2.5: Extract numeric year from string format
        df['year'] = extract_year_from_string(df['year'])
        df = df.dropna(subset=['year'])
        
        if df.empty:
            print(f"      ⚠️  No valid years after extraction, skipping")
            return None
        
        df['year'] = df['year'].astype(int)
        
        # Step 3: Handle scaling factors
        df = handle_scaling_factors(df)
        
        # Step 4: Keep only relevant columns
        df = keep_only_relevant_columns(df)
        
        # Step 5: Ensure year is integer (already done above)
        df['year'] = df['year'].astype(int)
        
        # Step 6: Handle missing values in numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_cols = [col for col in numeric_cols if col not in ['state', 'year']]
        
        for col in numeric_cols:
            # Forward fill within each state
            df[col] = df.groupby('state')[col].transform(lambda x: x.ffill())
            
            # Fill remaining NaN with column mean
            col_mean = df[col].mean()
            if pd.notna(col_mean):
                df[col] = df[col].fillna(col_mean)
        
        print(f"      ✅ Cleaned: {len(df)} rows, {len(df.columns)} columns")
        
        return df
    
    except Exception as e:
        print(f"   ❌ ERROR processing {filename}: {e}")
        return None


def merge_all_datasets(dfs):
    """
    Merge all cleaned dataframes on state + year using outer join.
    
    Args:
        dfs (list): List of cleaned DataFrames
        
    Returns:
        pd.DataFrame: Merged DataFrame
    """
    if not dfs:
        return pd.DataFrame()
    
    print(f"\n" + "="*70)
    print(f"🔗 MERGING ALL DATASETS")
    print("="*70)
    print(f"   Total datasets to merge: {len(dfs)}")
    
    # Start with first dataframe
    final_df = dfs[0].copy()
    print(f"   Starting with: {final_df.shape}")
    
    # Merge remaining dataframes
    for i, df in enumerate(dfs[1:], 2):
        before_rows = len(final_df)
        final_df = pd.merge(final_df, df, on=['state', 'year'], how='outer')
        after_rows = len(final_df)
        
        print(f"   [{i}/{len(dfs)}] Merged: {before_rows} → {after_rows} rows")
    
    print(f"\n   ✅ Final merged shape: {final_df.shape}")
    
    return final_df


def final_cleanup(df):
    """
    Final cleanup steps:
    1. Sort by state and year
    2. Forward fill per state
    3. Fill remaining with column mean
    4. Remove duplicate rows
    5. Add Country column (default 'IND' for India)
    
    Args:
        df (pd.DataFrame): Merged DataFrame
        
    Returns:
        pd.DataFrame: Final cleaned DataFrame
    """
    print(f"\n" + "="*70)
    print(f"🧹 FINAL CLEANUP")
    print("="*70)
    
    # Sort by state and year
    df = df.sort_values(['state', 'year']).reset_index(drop=True)
    print(f"   ✅ Sorted by state and year")
    
    # Forward fill within each state
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [col for col in numeric_cols if col not in ['state', 'year']]
    
    for col in numeric_cols:
        df[col] = df.groupby('state')[col].transform(lambda x: x.ffill())
    print(f"   ✅ Forward filled within states")
    
    # Fill remaining NaN with column mean
    for col in numeric_cols:
        col_mean = df[col].mean()
        if pd.notna(col_mean):
            df[col] = df[col].fillna(col_mean)
    print(f"   ✅ Filled remaining missing values with mean")
    
    # Remove duplicate rows
    before_count = len(df)
    df = df.drop_duplicates(subset=['state', 'year']).reset_index(drop=True)
    removed_dupes = before_count - len(df)
    if removed_dupes > 0:
        print(f"   ✅ Removed {removed_dupes} duplicate rows")
    else:
        print(f"   ✅ No duplicates found")
    
    # Add Country column
    if 'country' not in df.columns:
        df.insert(0, 'country', 'IND')
        print(f"   ✅ Added 'country' column (default: IND)")
    
    # Reorder columns
    identifier_cols = ['country', 'state', 'year']
    signal_cols = [col for col in df.columns if col not in identifier_cols]
    df = df[identifier_cols + signal_cols]
    
    return df


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main function to clean and merge infrastructure data."""
    
    # Check if data folder exists
    if not os.path.exists(DATA_PATH):
        print(f"\n❌ ERROR: Data folder not found: {DATA_PATH}")
        print(f"   Please run select_infrastructure_data.py first.")
        return
    
    # Get all CSV files
    csv_files = [f for f in os.listdir(DATA_PATH) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"\n⚠️  WARNING: No CSV files found in {DATA_PATH}")
        return
    
    print(f"\n📊 Found {len(csv_files)} file(s) to process\n")
    print("="*70)
    
    # Process each file
    dfs = []
    for i, filename in enumerate(sorted(csv_files), 1):
        filepath = os.path.join(DATA_PATH, filename)
        print(f"\n[{i}/{len(csv_files)}]")
        
        df = clean_single_file(filepath)
        
        if df is not None:
            dfs.append(df)
    
    print(f"\n" + "="*70)
    print(f"📊 PROCESSING SUMMARY")
    print("="*70)
    print(f"   Files processed: {len(dfs)}/{len(csv_files)}")
    
    if not dfs:
        print(f"\n❌ ERROR: No files were successfully processed")
        return
    
    # Merge all datasets
    final_df = merge_all_datasets(dfs)
    
    if final_df.empty:
        print(f"\n❌ ERROR: Merged DataFrame is empty")
        return
    
    # Final cleanup
    final_df = final_cleanup(final_df)
    
    # Calculate statistics
    total_cells = final_df.size
    missing_cells = final_df.isna().sum().sum()
    missing_pct = (missing_cells / total_cells) * 100 if total_cells > 0 else 0
    
    # Save output
    print(f"\n" + "="*70)
    print(f"💾 SAVING OUTPUT")
    print("="*70)
    
    final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   ✅ Saved to: {OUTPUT_FILE}")
    print(f"   📦 File size: {file_size:.2f} KB")
    
    # Print final summary
    print(f"\n" + "="*70)
    print(f"📊 FINAL DATASET SUMMARY")
    print("="*70)
    print(f"   Shape: {final_df.shape[0]} rows × {final_df.shape[1]} columns")
    print(f"   Number of states: {final_df['state'].nunique()}")
    print(f"   Year range: {final_df['year'].min()} - {final_df['year'].max()}")
    print(f"   Missing values: {missing_pct:.2f}%")
    
    # Show sample
    print(f"\n   📋 SAMPLE DATA (first 5 rows):")
    print(f"   {'-'*100}")
    print(final_df.head().to_string(index=False))
    print(f"   {'-'*100}")
    
    # Column overview
    print(f"\n   📋 COLUMNS ({len(final_df.columns)} total):")
    identifier_cols = ['country', 'state', 'year']
    signal_cols = [col for col in final_df.columns if col not in identifier_cols]
    print(f"      Identifiers: {', '.join(identifier_cols)}")
    print(f"      Signals: {len(signal_cols)} columns")
    if len(signal_cols) <= 20:
        print(f"         {', '.join(signal_cols)}")
    else:
        print(f"         First 10: {', '.join(signal_cols[:10])}")
        print(f"         ... and {len(signal_cols) - 10} more")
    
    print(f"\n" + "="*70)
    print(f"✅ INFRASTRUCTURE DATA CLEANING COMPLETE")
    print("="*70)
    print(f"\n📁 Next steps:")
    print(f"   1. Review output: {OUTPUT_FILE}")
    print(f"   2. Proceed to Phase 4: Feature Engineering")
    print(f"   3. Create infrastructure_features.py")
    print(f"\n" + "="*70)


# ================================================================
# 🚀 RUN
# ================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
