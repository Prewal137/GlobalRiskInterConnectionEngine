"""
🧹 Social Sector Data Cleaning Script

Merges all selected social datasets into one clean dataset: final_social.csv

Input: data/processed/social/selected/
Output: data/processed/social/cleaned/final_social.csv

Datasets:
1. Crimes dataset (sathwiksalian1515_17708937592866747.csv)
2. Case tracking dataset (sathwiksalian1515_17708937834479353.csv)
3. SC/ST crimes dataset (sathwiksalian1515_17708937963474379.csv)
4. Relief dataset (sathwiksalian1515_1770894338777049.csv)
"""

import pandas as pd
import os
import glob
import re

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 2 levels from pipeline/processing/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

INPUT_FOLDER = os.path.join(BASE_PATH, "data", "processed", "social", "selected")
OUTPUT_FOLDER = os.path.join(BASE_PATH, "data", "processed", "social", "cleaned")
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "final_social.csv")

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

print("="*70)
print("🧹 SOCIAL SECTOR DATA CLEANING")
print("="*70)
print(f"\n📂 Input folder: {INPUT_FOLDER}")
print(f"📁 Output folder: {OUTPUT_FOLDER}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def extract_year_from_string(year_str):
    """
    Extract year from various string formats.
    
    Examples:
    - "Calendar Year (Jan - Dec), 2022" → 2022
    - "Financial Year (Apr - Mar), 2022" → 2022
    - "2020" → 2020
    
    Args:
        year_str (str): Year string
        
    Returns:
        int: Extracted year or None if not found
    """
    try:
        # Try to find 4-digit year pattern
        match = re.search(r'\b(20\d{2}|19\d{2})\b', str(year_str))
        if match:
            return int(match.group(1))
        return None
    except:
        return None


def standardize_state_name(state_name):
    """
    Standardize state names for consistency.
    
    Args:
        state_name (str): Original state name
        
    Returns:
        str: Standardized state name
    """
    if pd.isna(state_name):
        return "Unknown"
    
    # Convert to title case and strip whitespace
    standardized = str(state_name).strip().title()
    
    # Handle special cases
    replacements = {
        'Nct Of Delhi': 'Delhi',
        'D&N Haveli': 'Dadra and Nagar Haveli',
        'Daman & Diu': 'Daman and Diu',
        'Jammu & Kashmir': 'Jammu and Kashmir',
    }
    
    return replacements.get(standardized, standardized)


def load_and_clean_dataset(filepath, dataset_name):
    """
    Load a single CSV file and standardize it.
    
    Args:
        filepath (str): Path to CSV file
        dataset_name (str): Name for logging
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with standard columns
    """
    print(f"\n📄 Loading: {dataset_name}")
    print(f"   File: {os.path.basename(filepath)}")
    
    try:
        df = pd.read_csv(filepath)
        print(f"   ✅ Loaded: {len(df):,} rows, {len(df.columns)} columns")
        
        # Standardize column names - keep only what we need
        columns_to_keep = []
        
        # Always keep Country, State, Year
        if 'Country' in df.columns:
            columns_to_keep.append('Country')
        else:
            df['Country'] = 'India'
            columns_to_keep.append('Country')
        
        # Find State column (might have different names)
        state_col = None
        for col in df.columns:
            if col.lower() in ['state', 'states or union territories (uts) or other']:
                state_col = col
                break
        
        if state_col:
            df = df.rename(columns={state_col: 'State'})
            columns_to_keep.append('State')
        else:
            print(f"   ⚠️  Warning: No State column found")
            df['State'] = 'Unknown'
            columns_to_keep.append('State')
        
        # Find and standardize Year column
        year_col = None
        for col in df.columns:
            if col.lower() == 'year':
                year_col = col
                break
        
        if year_col:
            # Extract year from string format if needed
            if df[year_col].dtype == 'object':
                print(f"   🔍 Extracting year from string format...")
                df['Year'] = df[year_col].apply(extract_year_from_string)
            else:
                df['Year'] = df[year_col]
            
            # Drop original year column and add standardized one
            if year_col != 'Year':
                df = df.drop(columns=[year_col])
            columns_to_keep.append('Year')
        else:
            print(f"   ⚠️  Warning: No Year column found")
            df['Year'] = None
            columns_to_keep.append('Year')
        
        # Extract core signals based on available columns
        signals = extract_core_signals(df, dataset_name)
        
        # Combine standard columns with signals
        final_columns = ['Country', 'State', 'Year'] + list(signals.keys())
        
        # Add signal columns to dataframe
        for col_name, values in signals.items():
            df[col_name] = values
        
        # Select only final columns
        df_clean = df[final_columns].copy()
        
        # Clean State names
        df_clean['State'] = df_clean['State'].apply(standardize_state_name)
        
        # Ensure Year is integer
        df_clean['Year'] = pd.to_numeric(df_clean['Year'], errors='coerce')
        df_clean = df_clean.dropna(subset=['Year'])
        df_clean['Year'] = df_clean['Year'].astype(int)
        
        # Fill missing numeric values with 0
        numeric_cols = df_clean.select_dtypes(include=['number']).columns
        df_clean[numeric_cols] = df_clean[numeric_cols].fillna(0)
        
        print(f"   ✅ Cleaned: {len(df_clean):,} rows")
        print(f"   Columns: {list(df_clean.columns)}")
        
        return df_clean
        
    except Exception as e:
        print(f"   ❌ ERROR loading {dataset_name}: {str(e)}")
        return pd.DataFrame()


def extract_core_signals(df, dataset_name):
    """
    Extract core social risk signals from dataset.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        dataset_name (str): Name of dataset for column mapping
        
    Returns:
        dict: Dictionary of signal columns
    """
    signals = {
        'protest_count': 0,
        'violence_count': 0,
        'conflict_events': 0,
        'fatalities': 0
    }
    
    # Map columns based on dataset content
    for col in df.columns:
        col_lower = col.lower()
        
        # Protest count: Number of cases registered
        if 'number of cases registered during the year' in col_lower:
            signals['protest_count'] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Violence count: Number of crimes or atrocities
        elif 'number of crimes or atrocities' in col_lower:
            signals['violence_count'] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Conflict events: Total number of cases registered
        elif 'total number of cases registered' in col_lower:
            signals['conflict_events'] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Also check for SC/ST specific registrations
        elif 'number of cases registered on scheduled caste' in col_lower:
            sc_cases = pd.to_numeric(df[col], errors='coerce').fillna(0)
            signals['protest_count'] = signals['protest_count'] + sc_cases
        
        elif 'number of cases registered on scheduled tribe' in col_lower:
            st_cases = pd.to_numeric(df[col], errors='coerce').fillna(0)
            signals['protest_count'] = signals['protest_count'] + st_cases
    
    return signals


def merge_datasets(dataframes_dict):
    """
    Merge all datasets using OUTER JOIN on State + Year.
    
    Args:
        dataframes_dict (dict): Dictionary of DataFrames by dataset name
        
    Returns:
        pd.DataFrame: Merged DataFrame
    """
    print("\n" + "="*70)
    print("🔗 Merging datasets...")
    print("="*70)
    
    merged_df = None
    
    for dataset_name, df in dataframes_dict.items():
        if df.empty:
            print(f"   ⏭️  Skipping empty dataset: {dataset_name}")
            continue
        
        print(f"\n   Merging: {dataset_name} ({len(df):,} rows)")
        
        if merged_df is None:
            # First dataset
            merged_df = df.copy()
            print(f"      ✓ Initial dataset loaded")
        else:
            # Merge with existing
            before_rows = len(merged_df)
            
            merged_df = merged_df.merge(
                df,
                on=['Country', 'State', 'Year'],
                how='outer',
                suffixes=('', f'_{dataset_name}')
            )
            
            after_rows = len(merged_df)
            print(f"      ✓ Merged: {before_rows:,} → {after_rows:,} rows")
    
    if merged_df is None:
        print("   ❌ ERROR: No datasets to merge")
        return pd.DataFrame()
    
    return merged_df


def resolve_duplicate_columns(df):
    """
    Resolve duplicate columns after merge by summing them.
    
    Args:
        df (pd.DataFrame): DataFrame with potential duplicate columns
        
    Returns:
        pd.DataFrame: DataFrame with resolved columns
    """
    print("\n🔧 Resolving duplicate columns...")
    
    # Define the standard signal column names we want to keep
    standard_signals = ['protest_count', 'violence_count', 'conflict_events', 'fatalities']
    
    columns_to_drop = []
    
    # For each standard signal, find all matching columns and sum them
    for signal in standard_signals:
        # Find all columns that start with this signal name (but not exactly the signal name)
        matching_cols = [col for col in df.columns if col.startswith(signal) and col != signal]
        
        if len(matching_cols) > 0:
            # There are duplicate/suffixed columns - need to sum them with the base column
            cols_to_sum = [signal] + matching_cols if signal in df.columns else matching_cols
            
            if len(cols_to_sum) > 1:
                # Multiple columns - sum them
                print(f"   Summing {len(cols_to_sum)} columns for '{signal}'")
                df[signal] = df[cols_to_sum].sum(axis=1)
            
            # Mark suffixed columns for deletion (not the base signal column)
            columns_to_drop.extend(matching_cols)
        elif signal not in df.columns:
            print(f"   ⚠️  Warning: '{signal}' not found in dataframe")
    
    # Drop all the old duplicate columns at once
    if columns_to_drop:
        print(f"   Dropping {len(columns_to_drop)} duplicate columns")
        df = df.drop(columns=columns_to_drop)
    
    print(f"   Columns after resolution: {list(df.columns)}")
    
    return df


def final_cleanup(df):
    """
    Final cleanup steps.
    
    Args:
        df (pd.DataFrame): Merged DataFrame
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n🧹 Final cleanup...")
    
    # Fill remaining NaN values with 0
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # Remove duplicates
    before_rows = len(df)
    df = df.drop_duplicates(subset=['Country', 'State', 'Year'])
    after_rows = len(df)
    
    if before_rows != after_rows:
        print(f"   Removed {before_rows - after_rows} duplicate rows")
    
    # Sort by State and Year
    df = df.sort_values(['State', 'Year']).reset_index(drop=True)
    
    # Ensure correct dtypes
    df['Country'] = df['Country'].astype(str)
    df['State'] = df['State'].astype(str)
    df['Year'] = df['Year'].astype(int)
    
    # Add Month column (default = 1 for annual data)
    # This is needed for time-series features later
    if 'Month' not in df.columns:
        print(f"   ➕ Adding Month column (default = 1 for annual data)")
        df['Month'] = 1
    
    # Reorder columns: Country, State, Year, Month, then signals
    signal_cols = ['protest_count', 'violence_count', 'conflict_events', 'fatalities']
    existing_signals = [col for col in signal_cols if col in df.columns]
    column_order = ['Country', 'State', 'Year', 'Month'] + existing_signals
    df = df[column_order]
    
    # Ensure correct dtypes for signal columns
    for col in existing_signals:
        if col in df.columns:
            # Keep as float to preserve decimal values from aggregation
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Data quality check - show descriptive statistics
    print(f"\n📊 Data Quality Check (Descriptive Statistics):")
    print(f"   Columns after cleanup: {list(df.columns)}")
    print(f"   ✅ Final shape: {df.shape}")
    
    # Show stats for numeric columns
    numeric_df = df.select_dtypes(include=['number'])
    if not numeric_df.empty:
        desc = numeric_df.describe()
        print(f"\n   📈 Signal Statistics:")
        for col in existing_signals:
            if col in desc.columns:
                mean_val = desc[col]['mean']
                std_val = desc[col]['std']
                max_val = desc[col]['max']
                min_val = desc[col]['min']
                
                # Check for extreme outliers (values > 3 standard deviations)
                threshold = mean_val + 3 * std_val
                outliers = (df[col] > threshold).sum()
                
                status = "⚠️" if outliers > 0 else "✅"
                print(f"      {status} {col:<20s}: mean={mean_val:>8.1f}, std={std_val:>8.1f}, "
                      f"min={min_val:>8.1f}, max={max_val:>8.1f}, outliers={outliers}")
        
        if outliers > 0:
            print(f"\n   ⚠️  Note: Some extreme values detected. Consider smoothing in feature engineering phase.")
        else:
            print(f"\n   ✅ No extreme outliers detected")
    
    return df


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    # Check if input folder exists
    if not os.path.exists(INPUT_FOLDER):
        print(f"\n❌ ERROR: Input folder not found: {INPUT_FOLDER}")
        return
    
    # Get all CSV files
    csv_files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv"))
    
    if not csv_files:
        print(f"\n⚠️  WARNING: No CSV files found in {INPUT_FOLDER}")
        return
    
    print(f"\n📊 Found {len(csv_files)} CSV file(s)")
    print("="*70)
    
    # Load and clean each dataset
    datasets = {}
    
    for filepath in sorted(csv_files):
        filename = os.path.basename(filepath)
        
        # Assign dataset name based on filename
        if '17708937592866747' in filename:
            dataset_name = "crimes"
        elif '17708937834479353' in filename:
            dataset_name = "case_tracking"
        elif '17708937963474379' in filename:
            dataset_name = "scst_crimes"
        elif '1770894338777049' in filename:
            dataset_name = "relief"
        else:
            dataset_name = f"unknown_{filename[:20]}"
        
        df = load_and_clean_dataset(filepath, dataset_name)
        
        if not df.empty:
            datasets[dataset_name] = df
    
    if not datasets:
        print("\n❌ ERROR: No valid datasets loaded")
        return
    
    print("\n" + "="*70)
    print("📊 LOADED DATASETS SUMMARY")
    print("="*70)
    for name, df in datasets.items():
        print(f"   • {name:<20s}: {len(df):>6,} rows, {len(df.columns)} columns")
    
    # Merge all datasets
    merged_df = merge_datasets(datasets)
    
    if merged_df.empty:
        print("\n❌ ERROR: Merge resulted in empty dataset")
        return
    
    # Resolve duplicate columns
    merged_df = resolve_duplicate_columns(merged_df)
    
    # Final cleanup
    final_df = final_cleanup(merged_df)
    
    # ================================================================
    # 💾 SAVE OUTPUT
    # ================================================================
    
    print("\n" + "="*70)
    print("💾 Saving output...")
    
    final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   ✅ Output saved to: {OUTPUT_FILE}")
    print(f"   📦 File size: {file_size:.2f} KB")
    
    # Print summary statistics
    print("\n" + "="*70)
    print("📊 FINAL DATASET SUMMARY")
    print("="*70)
    print(f"   Total rows: {len(final_df):,}")
    print(f"   Total states: {final_df['State'].nunique()}")
    print(f"   Year range: {final_df['Year'].min()} - {final_df['Year'].max()}")
    if 'Month' in final_df.columns:
        print(f"   Month: All set to 1 (annual data)")
    print(f"   Columns: {list(final_df.columns)}")
    
    # Show sample
    print(f"\n📄 Sample data (first 5 rows):")
    print(final_df.head().to_string(index=False))
    
    print("\n" + "="*70)
    print("✅ Social data cleaning completed")
    print("="*70)


if __name__ == "__main__":
    main()
