"""
🧹 Migration Sector Data Cleaner

Converts World Bank wide-format API files into unified time-series dataset.

Input:
    - data/raw/migration/API_*.csv (World Bank format)

Output:
    - data/processed/migration/final_migration.csv

Process:
    1. Load all API CSV files
    2. Filter for India (IND)
    3. Reshape wide → long format using melt()
    4. Rename indicators to meaningful names
    5. Merge all indicators on Country + Year
    6. Clean and interpolate missing values
    7. Save final unified dataset
"""

import pandas as pd
import numpy as np
import os
import glob

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
RAW_MIGRATION_PATH = os.path.join(BASE_PATH, "data", "raw", "migration")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "migration", "final_migration.csv")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def get_api_files(path):
    """
    Get all API CSV files from migration directory.
    
    Args:
        path (str): Path to migration data directory
        
    Returns:
        list: List of file paths
    """
    pattern = os.path.join(path, "API_*.csv")
    files = glob.glob(pattern)
    return sorted(files)


def load_and_filter_country(filepath, country_code="IND"):
    """
    Load World Bank CSV and filter for specific country.
    
    Args:
        filepath (str): Path to CSV file
        country_code (str): Country code to filter (default: IND)
        
    Returns:
        pd.DataFrame or None: Filtered DataFrame
    """
    try:
        # Skip first 3 rows (metadata notes in World Bank files)
        df = pd.read_csv(filepath, skiprows=3)
        
        # Filter for specific country
        if 'Country Code' in df.columns:
            df_filtered = df[df['Country Code'] == country_code].copy()
            
            if df_filtered.empty:
                print(f"      ⚠️  {country_code} not found in {os.path.basename(filepath)}")
                return None
            
            return df_filtered
        else:
            print(f"      ⚠️  'Country Code' column not found")
            return None
            
    except Exception as e:
        print(f"      ❌ Error loading {os.path.basename(filepath)}: {e}")
        return None


def reshape_wide_to_long(df, value_name="Value"):
    """
    Convert World Bank wide format to long format.
    
    Wide format:
        Country | Country Code | Indicator | 1960 | 1961 | ... | 2025
    
    Long format:
        Country | Country Code | Indicator | Year | Value
    
    Args:
        df (pd.DataFrame): Wide format DataFrame
        value_name (str): Name for the value column
        
    Returns:
        pd.DataFrame: Long format DataFrame
    """
    # Identify year columns (4-digit numbers)
    year_cols = [col for col in df.columns if str(col).strip().isdigit() and len(str(col).strip()) == 4]
    
    if not year_cols:
        print(f"      ⚠️  No year columns found")
        return None
    
    # Columns to keep as identifiers
    id_cols = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']
    id_cols = [col for col in id_cols if col in df.columns]
    
    # Melt the DataFrame
    df_long = pd.melt(
        df,
        id_vars=id_cols,
        value_vars=year_cols,
        var_name='Year',
        value_name=value_name
    )
    
    # Convert Year to integer
    df_long['Year'] = df_long['Year'].astype(int)
    
    # Sort by Year
    df_long = df_long.sort_values('Year').reset_index(drop=True)
    
    return df_long


def detect_indicator_type(df):
    """
    Detect what type of indicator this file contains based on Indicator Name.
    
    Args:
        df (pd.DataFrame): DataFrame with Indicator Name column
        
    Returns:
        str: Indicator type name
    """
    if df.empty or 'Indicator Name' not in df.columns:
        return "Unknown"
    
    indicator_name = df['Indicator Name'].iloc[0].lower()
    
    if 'unemployment' in indicator_name:
        return "Unemployment"
    elif 'net migration' in indicator_name or 'migration' in indicator_name:
        return "Migration"
    elif 'population growth' in indicator_name and 'urban' not in indicator_name:
        return "PopulationGrowth"
    elif 'urban' in indicator_name and 'growth' in indicator_name:
        return "UrbanGrowth"
    else:
        # Use first few words as fallback
        words = indicator_name.split()[:2]
        return ''.join(words).title()


def clean_and_interpolate(df, value_col):
    """
    Clean data and interpolate missing values.
    
    Args:
        df (pd.DataFrame): DataFrame with missing values
        value_col (str): Column name to interpolate
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    df_clean = df.copy()
    
    # Count missing before
    missing_before = df_clean[value_col].isna().sum()
    
    if missing_before > 0:
        print(f"         Interpolating {missing_before} missing values...")
        
        # Convert to numeric (in case of strings)
        df_clean[value_col] = pd.to_numeric(df_clean[value_col], errors='coerce')
        
        # Interpolate missing values (linear interpolation)
        df_clean[value_col] = df_clean[value_col].interpolate(method='linear')
        
        # Handle any remaining NaN at edges (forward/backward fill)
        df_clean[value_col] = df_clean[value_col].ffill().bfill()
        
        # Count missing after
        missing_after = df_clean[value_col].isna().sum()
        print(f"         Missing after interpolation: {missing_after}")
    
    return df_clean


# ================================================================
# 🎯 MAIN CLEANING PIPELINE
# ================================================================

def clean_migration_data():
    """
    Main pipeline to clean and unify migration data.
    
    Returns:
        pd.DataFrame: Final cleaned dataset
    """
    print("\n" + "="*80)
    print("🧹 MIGRATION SECTOR DATA CLEANING")
    print("="*80)
    
    # ========================================
    # STEP 1: LOAD FILES
    # ========================================
    print("\n📥 STEP 1: LOADING API FILES")
    print("-"*80)
    
    api_files = get_api_files(RAW_MIGRATION_PATH)
    
    if not api_files:
        print(f"❌ No API_*.csv files found in {RAW_MIGRATION_PATH}")
        return pd.DataFrame()
    
    print(f"   Found {len(api_files)} API files")
    
    # ========================================
    # STEP 2-4: LOAD, FILTER, RESHAPE EACH FILE
    # ========================================
    print("\n🔄 STEP 2-4: PROCESSING INDICATORS")
    print("-"*80)
    
    indicator_dfs = {}
    
    for i, filepath in enumerate(api_files, 1):
        filename = os.path.basename(filepath)
        print(f"\n[{i}/{len(api_files)}] Processing: {filename}")
        
        # Step 2: Load and filter for India
        df_country = load_and_filter_country(filepath, country_code="IND")
        
        if df_country is None:
            continue
        
        print(f"      ✅ Loaded India data ({len(df_country)} row)")
        
        # Step 3: Reshape wide → long
        indicator_type = detect_indicator_type(df_country)
        df_long = reshape_wide_to_long(df_country, value_name=indicator_type)
        
        if df_long is None:
            continue
        
        print(f"      ✅ Reshaped to long format ({len(df_long)} rows)")
        print(f"      ✅ Indicator: {indicator_type}")
        print(f"      ✅ Year range: {df_long['Year'].min()} - {df_long['Year'].max()}")
        
        # Step 6: Clean and interpolate
        df_clean = clean_and_interpolate(df_long, indicator_type)
        
        # Store for merging
        indicator_dfs[indicator_type] = df_clean[['Country Code', 'Year', indicator_type]]
    
    if not indicator_dfs:
        print("\n❌ No valid indicators processed")
        return pd.DataFrame()
    
    print(f"\n   ✅ Processed {len(indicator_dfs)} indicators:")
    for ind_name in indicator_dfs.keys():
        print(f"      - {ind_name}")
    
    # ========================================
    # STEP 5: MERGE ALL INDICATORS
    # ========================================
    print("\n🔗 STEP 5: MERGING INDICATORS")
    print("-"*80)
    
    # Start with first indicator
    merged_df = None
    merge_count = 0
    
    for ind_name, df_ind in sorted(indicator_dfs.items()):
        if merged_df is None:
            merged_df = df_ind.copy()
            print(f"   Starting with: {ind_name} ({len(merged_df)} rows)")
        else:
            # Merge on Country Code + Year
            before_merge = len(merged_df)
            merged_df = merged_df.merge(
                df_ind,
                on=['Country Code', 'Year'],
                how='outer'  # Keep all years, even if some indicators missing
            )
            after_merge = len(merged_df)
            merge_count += 1
            print(f"   Merged {ind_name}: {before_merge} → {after_merge} rows")
    
    if merged_df is None:
        print("\n❌ Merge failed")
        return pd.DataFrame()
    
    print(f"\n   ✅ Final merged dataset: {len(merged_df)} rows")
    
    # ========================================
    # STEP 6: FINAL CLEANING
    # ========================================
    print("\n🧼 STEP 6: FINAL CLEANING")
    print("-"*80)
    
    # Rename Country Code to Country
    merged_df = merged_df.rename(columns={'Country Code': 'Country'})
    
    # Ensure correct data types
    merged_df['Year'] = merged_df['Year'].astype(int)
    
    # Convert all indicator columns to float
    indicator_cols = [col for col in merged_df.columns if col not in ['Country', 'Year']]
    for col in indicator_cols:
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
    
    # Sort by Year
    merged_df = merged_df.sort_values(['Country', 'Year']).reset_index(drop=True)
    
    # Report missing values
    print(f"   Missing values per column:")
    for col in indicator_cols:
        missing = merged_df[col].isna().sum()
        total = len(merged_df)
        pct = (missing / total * 100) if total > 0 else 0
        print(f"      {col:25s}: {missing:4d} ({pct:5.1f}%)")
    
    # ========================================
    # STEP 7: VALIDATE FINAL STRUCTURE
    # ========================================
    print("\n✅ STEP 7: VALIDATION")
    print("-"*80)
    
    expected_columns = ['Country', 'Year']
    actual_indicators = [col for col in merged_df.columns if col not in ['Country', 'Year']]
    
    print(f"   Expected structure: Country, Year, [indicators...]")
    print(f"   Actual columns: {list(merged_df.columns)}")
    print(f"   Total rows: {len(merged_df)}")
    print(f"   Year range: {merged_df['Year'].min()} - {merged_df['Year'].max()}")
    print(f"   Countries: {merged_df['Country'].unique()}")
    
    # Check for any completely empty rows
    empty_rows = merged_df[indicator_cols].isna().all(axis=1).sum()
    if empty_rows > 0:
        print(f"   ⚠️  Dropping {empty_rows} rows with all NaN indicators")
        merged_df = merged_df.dropna(subset=indicator_cols, how='all').reset_index(drop=True)
        print(f"   ✅ Final rows after cleanup: {len(merged_df)}")
    
    # ========================================
    # STEP 8: SAVE OUTPUT
    # ========================================
    print("\n💾 STEP 8: SAVING OUTPUT")
    print("-"*80)
    
    merged_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   ✅ Saved to: {OUTPUT_FILE}")
    print(f"   📦 File size: {file_size:.2f} KB")
    print(f"   📊 Rows: {len(merged_df)}")
    print(f"   📋 Columns: {len(merged_df.columns)}")
    
    # Preview
    print(f"\n📄 PREVIEW (first 10 rows):")
    print("-"*80)
    print(merged_df.head(10).to_string(index=False))
    print("-"*80)
    
    return merged_df


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    try:
        df_final = clean_migration_data()
        
        if not df_final.empty:
            print("\n" + "="*80)
            print("🎯 MIGRATION CLEANING COMPLETE")
            print("="*80)
            print(f"\n✅ Ready for feature engineering and modeling!")
            print(f"📁 Output: {OUTPUT_FILE}")
            print("="*80)
        else:
            print("\n❌ Cleaning failed - no output generated")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
