"""
🧹 Economy Sector Data Cleaner - MONTHLY Edition (Phase 3 Upgrade)

This script cleans and unifies economy datasets into a MONTHLY time-series format.

Input: data/raw/economy/
Output: data/processed/economy/final_economy_monthly.csv

Key Features:
- Converts all data to monthly frequency
- Uses interpolation (NOT forward fill) for missing values
- Produces ML-ready monthly dataset
"""

import pandas as pd
import os
import glob
import numpy as np
from pathlib import Path

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
RAW_ECONOMY_PATH = os.path.join(BASE_PATH, "data", "raw", "economy")
PROCESSED_ECONOMY_PATH = os.path.join(BASE_PATH, "data", "processed", "economy")
OUTPUT_FILE = os.path.join(PROCESSED_ECONOMY_PATH, "final_economy_monthly.csv")

# Ensure output directory exists
os.makedirs(PROCESSED_ECONOMY_PATH, exist_ok=True)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def standardize_date_column(df, date_col):
    """
    Standardize date column to datetime format.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        date_col (str): Name of date column
        
    Returns:
        pd.DataFrame: DataFrame with standardized datetime
    """
    df = df.copy()
    
    try:
        # Try different date formats
        if date_col in df.columns:
            # First try standard parsing
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
            
            # If still has NaT, try more flexible parsing
            if df[date_col].isna().any():
                df[date_col] = pd.to_datetime(df[date_col], infer_datetime_format=True, errors='coerce')
                
    except Exception as e:
        print(f"      ⚠️  Date parsing warning: {e}")
    
    return df


def extract_year_month(df, date_col):
    """
    Extract Year and Month from date column.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        date_col (str): Name of date column
        
    Returns:
        pd.DataFrame: DataFrame with Year and Month columns
    """
    df = df.copy()
    
    if date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df['Year'] = df[date_col].dt.year
        df['Month'] = df[date_col].dt.month
    
    return df


def clean_column_name(col):
    """
    Clean column names by stripping spaces.
    
    Args:
        col (str): Column name
        
    Returns:
        str: Cleaned column name
    """
    return str(col).strip()


# ================================================================
# 🔧 DATA LOADING FUNCTIONS
# ================================================================

def load_exchange_rate_monthly():
    """
    Load exchange rate data and convert to monthly frequency.
    Daily data → Monthly mean
    
    Returns:
        pd.DataFrame: Monthly exchange rate data
    """
    print("\n💱 Loading Exchange Rate (DEXINUS) - Monthly...")
    
    try:
        df = pd.read_csv(os.path.join(RAW_ECONOMY_PATH, "DEXINUS.csv"))
        
        # Standardize date
        df = standardize_date_column(df, 'observation_date')
        df = extract_year_month(df, 'observation_date')
        
        # Aggregate to monthly mean
        monthly_df = df.groupby(['Year', 'Month'])['DEXINUS'].mean().reset_index()
        monthly_df.columns = ['Year', 'Month', 'ExchangeRate']
        
        # Add country
        monthly_df['Country'] = 'IND'
        
        n_months = len(monthly_df)
        print(f"   ✅ Converted to {n_months} months ({monthly_df['Year'].min()}-{monthly_df['Year'].max()})")
        print(f"   Mean Exchange Rate: {monthly_df['ExchangeRate'].mean():.4f}")
        
        return monthly_df[['Country', 'Year', 'Month', 'ExchangeRate']]
        
    except Exception as e:
        print(f"   ❌ Error loading exchange rate: {e}")
        return None


def load_inflation_monthly():
    """
    Load inflation data (CPI) - already monthly.
    Calculates inflation as YoY change.
    
    Returns:
        pd.DataFrame: Monthly inflation data
    """
    print("\n📈 Loading Inflation (CPI) - Monthly...")
    
    try:
        df = pd.read_csv(os.path.join(RAW_ECONOMY_PATH, "INDCPIALLMINMEI.csv"))
        
        # Standardize date
        df = standardize_date_column(df, 'observation_date')
        df = extract_year_month(df, 'observation_date')
        
        # Keep raw monthly CPI data
        monthly_cpi = df[['Year', 'Month', 'INDCPIALLMINMEI']].copy()
        
        # Sort by time
        monthly_cpi = monthly_cpi.sort_values(['Year', 'Month']).reset_index(drop=True)
        
        # Calculate YoY inflation (% change from same month previous year)
        # This compares each month to the same month in the previous year
        monthly_cpi['Inflation'] = monthly_cpi['INDCPIALLMINMEI'].pct_change(periods=12) * 100
        
        # Store CPI level separately
        monthly_cpi['CPI_Level'] = monthly_cpi['INDCPIALLMINMEI']
        
        # Drop rows where we can't calculate YoY (first 12 months)
        inflation_df = monthly_cpi.dropna(subset=['Inflation'])[['Year', 'Month', 'Inflation']].copy()
        inflation_df['Country'] = 'IND'
        
        n_months = len(inflation_df)
        print(f"   ✅ Loaded {n_months} months ({inflation_df['Year'].min()}-{inflation_df['Year'].max()})")
        print(f"   Mean Inflation: {inflation_df['Inflation'].mean():.2f}%")
        
        return inflation_df[['Country', 'Year', 'Month', 'Inflation']]
        
    except Exception as e:
        print(f"   ❌ Error loading inflation: {e}")
        return None


def load_interest_rate_monthly():
    """
    Load interest rate data and convert to monthly frequency.
    
    Returns:
        pd.DataFrame: Monthly interest rate data
    """
    print("\n🏦 Loading Interest Rate - Monthly...")
    
    try:
        df = pd.read_csv(os.path.join(RAW_ECONOMY_PATH, "INDIRLTLT01STM.csv"))
        
        # Standardize date
        df = standardize_date_column(df, 'observation_date')
        df = extract_year_month(df, 'observation_date')
        
        # Aggregate to monthly mean
        monthly_df = df.groupby(['Year', 'Month'])['INDIRLTLT01STM'].mean().reset_index()
        monthly_df.columns = ['Year', 'Month', 'InterestRate']
        
        monthly_df['Country'] = 'IND'
        
        n_months = len(monthly_df)
        print(f"   ✅ Converted to {n_months} months ({monthly_df['Year'].min()}-{monthly_df['Year'].max()})")
        print(f"   Mean Interest Rate: {monthly_df['InterestRate'].mean():.2f}%")
        
        return monthly_df[['Country', 'Year', 'Month', 'InterestRate']]
        
    except Exception as e:
        print(f"   ❌ Error loading interest rate: {e}")
        return None


def load_nifty50_monthly():
    """
    Load NIFTY 50 daily data and aggregate to monthly.
    Daily Close prices → Monthly mean
    
    Returns:
        pd.DataFrame: Monthly NIFTY 50 data
    """
    print("\n📈 Loading NIFTY 50 - Monthly...")
    
    try:
        # Find all NIFTY files
        nifty_files = glob.glob(os.path.join(RAW_ECONOMY_PATH, "NIFTY 50_Historical_PR_*.csv"))
        
        if not nifty_files:
            print("   ❌ No NIFTY files found")
            return None
        
        all_data = []
        
        for file_path in sorted(nifty_files):
            try:
                df = pd.read_csv(file_path)
                
                # Clean column names
                df.columns = [clean_column_name(col) for col in df.columns]
                
                # Standardize date
                if 'Date' in df.columns:
                    df = standardize_date_column(df, 'Date')
                    df = extract_year_month(df, 'Date')
                    
                    # Use Close price
                    if 'Close' in df.columns:
                        # Aggregate to monthly mean
                        monthly_close = df.groupby(['Year', 'Month'])['Close'].mean().reset_index()
                        monthly_close.columns = ['Year', 'Month', 'NIFTY50']
                        all_data.append(monthly_close)
                        
            except Exception as e:
                continue
        
        if len(all_data) == 0:
            print("   ❌ No valid NIFTY data extracted")
            return None
        
        # Combine all months
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates by taking mean
        monthly_df = combined_df.groupby(['Year', 'Month'])['NIFTY50'].mean().reset_index()
        monthly_df['Country'] = 'IND'
        
        n_months = len(monthly_df)
        print(f"   ✅ Converted to {n_months} months ({monthly_df['Year'].min()}-{monthly_df['Year'].max()})")
        print(f"   Mean NIFTY 50: {monthly_df['NIFTY50'].mean():.2f}")
        
        return monthly_df[['Country', 'Year', 'Month', 'NIFTY50']]
        
    except Exception as e:
        print(f"   ❌ Error loading NIFTY 50: {e}")
        return None


def load_vix_monthly():
    """
    Load VIX daily data and aggregate to monthly.
    Daily Close prices → Monthly mean
    
    Returns:
        pd.DataFrame: Monthly VIX data
    """
    print("\n📉 Loading VIX - Monthly...")
    
    try:
        # Find all VIX files
        vix_files = glob.glob(os.path.join(RAW_ECONOMY_PATH, "hist_india_vix_*.csv"))
        
        if not vix_files:
            print("   ❌ No VIX files found")
            return None
        
        all_data = []
        
        for file_path in sorted(vix_files):
            try:
                df = pd.read_csv(file_path)
                
                # Clean column names
                df.columns = [clean_column_name(col) for col in df.columns]
                
                # Standardize date
                if 'Date' in df.columns:
                    df = standardize_date_column(df, 'Date')
                    df = extract_year_month(df, 'Date')
                    
                    # Use Close price as VIX level
                    if 'Close' in df.columns:
                        # Aggregate to monthly mean
                        monthly_vix = df.groupby(['Year', 'Month'])['Close'].mean().reset_index()
                        monthly_vix.columns = ['Year', 'Month', 'VIX']
                        all_data.append(monthly_vix)
                        
            except Exception as e:
                continue
        
        if len(all_data) == 0:
            print("   ❌ No valid VIX data extracted")
            return None
        
        # Combine all months
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates by taking mean
        monthly_df = combined_df.groupby(['Year', 'Month'])['VIX'].mean().reset_index()
        monthly_df['Country'] = 'IND'
        
        n_months = len(monthly_df)
        print(f"   ✅ Converted to {n_months} months ({monthly_df['Year'].min()}-{monthly_df['Year'].max()})")
        print(f"   Mean VIX: {monthly_df['VIX'].mean():.2f}")
        
        return monthly_df[['Country', 'Year', 'Month', 'VIX']]
        
    except Exception as e:
        print(f"   ❌ Error loading VIX: {e}")
        return None


# ================================================================
# 🔧 MERGING AND CLEANING
# ================================================================

def merge_datasets(datasets):
    """
    Merge all datasets on Country + Year + Month.
    
    Args:
        datasets (list): List of DataFrames to merge
        
    Returns:
        pd.DataFrame: Merged dataset
    """
    print("\n🔗 Merging datasets on Country+Year+Month...")
    
    # First, combine all datasets into one DataFrame
    all_data = []
    
    for df in datasets:
        all_data.append(df)
    
    # Concatenate all datasets
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Remove duplicates by taking mean for each Country+Year+Month combination
    grouped = combined_df.groupby(['Country', 'Year', 'Month'])
    
    # Aggregate numeric columns by mean
    merged_df = grouped.mean(numeric_only=True).reset_index()
    
    # Add back Country column (lost in groupby)
    merged_df['Country'] = combined_df.groupby(['Country', 'Year', 'Month'])['Country'].first().values
    
    print(f"   Combined {len(combined_df)} total rows")
    print(f"   After deduplication: {len(merged_df)} unique Country+Year+Month combinations")
    
    # Report completeness for each feature
    feature_cols = [col for col in merged_df.columns if col not in ['Country', 'Year', 'Month']]
    for col in feature_cols:
        print(f"   {col}: {merged_df[col].notna().sum()} non-null values")
    
    return merged_df


def sort_by_time(df):
    """
    Sort dataset by Country, Year, Month.
    
    Args:
        df (pd.DataFrame): Dataset
        
    Returns:
        pd.DataFrame: Sorted dataset
    """
    print("\n📊 Sorting by Country, Year, Month...")
    
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    print(f"   ✅ Sorted: {df['Year'].min()}-{df['Year'].max()} (Months: {df['Month'].min()}-{df['Month'].max()})")
    
    return df


def handle_missing_values(df):
    """
    Handle missing values using LINEAR INTERPOLATION (NOT forward fill).
    
    Args:
        df (pd.DataFrame): Dataset with potential missing values
        
    Returns:
        pd.DataFrame: Cleaned dataset
    """
    print("\n🔧 Handling missing values with LINEAR INTERPOLATION...")
    
    # Sort by time first (critical for proper interpolation)
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    # For each country separately
    countries = df['Country'].unique()
    
    cleaned_dfs = []
    
    for country in countries:
        country_df = df[df['Country'] == country].copy()
        
        # Step 1: Linear interpolation (PRIMARY METHOD)
        numeric_cols = country_df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if country_df[col].isna().any():
                country_df[col] = country_df[col].interpolate(method='linear')
        
        # Step 2: Forward fill ONLY as fallback for remaining NaNs
        country_df = country_df.ffill()
        
        # Step 3: Backward fill for any leading NaNs
        country_df = country_df.bfill()
        
        cleaned_dfs.append(country_df)
    
    result_df = pd.concat(cleaned_dfs, ignore_index=True)
    
    # Report missing values
    missing_before = df.isna().sum().sum()
    missing_after = result_df.isna().sum().sum()
    
    print(f"   Missing values before: {missing_before}")
    print(f"   Missing values after: {missing_after}")
    
    # Drop rows that are still too empty (more than 50% features missing)
    feature_cols = [col for col in result_df.columns if col not in ['Country', 'Year', 'Month']]
    min_features = len(feature_cols) // 2
    
    mask = result_df[feature_cols].notna().sum(axis=1) >= min_features
    result_df = result_df[mask].reset_index(drop=True)
    
    print(f"   Rows after dropping sparse data: {len(result_df)}")
    
    return result_df


def finalize_data_types(df):
    """
    Ensure correct data types and filter to safe time period.
    
    Args:
        df (pd.DataFrame): Dataset
        
    Returns:
        pd.DataFrame: Dataset with proper types, filtered to 2010-2024
    """
    print("\n🔧 Finalizing data types and filtering time range...")
    
    # CRITICAL FIX: Remove 2026 fake/interpolated data
    # Only keep real data (2010-2024)
    original_rows = len(df)
    df = df[df['Year'] <= 2024].reset_index(drop=True)
    removed_rows = original_rows - len(df)
    
    if removed_rows > 0:
        print(f"   ⚠️  Removed {removed_rows} rows from 2025-2026 (fake/interpolated data)")
        print(f"   ✅ Kept {len(df)} rows from 2010-2024 (REAL DATA ONLY)")
    
    # Year and Month as integers
    df['Year'] = df['Year'].astype(int)
    df['Month'] = df['Month'].astype(int)
    
    # All features as float
    feature_cols = [col for col in df.columns if col not in ['Country', 'Year', 'Month']]
    for col in feature_cols:
        df[col] = df[col].astype(float)
    
    # Country as string
    df['Country'] = df['Country'].astype(str)
    
    print("   ✅ Data types finalized")
    
    return df


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    print("\n" + "="*70)
    print("🧹 ECONOMY SECTOR DATA CLEANING - MONTHLY EDITION")
    print("="*70)
    
    # ============================================================
    # STEP 1: LOAD ALL DATASETS
    # ============================================================
    
    print("\n📥 STEP 1: Loading individual datasets...")
    print("-" * 60)
    
    datasets = []
    
    # Load each dataset
    exchange_df = load_exchange_rate_monthly()
    if exchange_df is not None:
        datasets.append(exchange_df)
    
    inflation_df = load_inflation_monthly()
    if inflation_df is not None:
        datasets.append(inflation_df)
    
    interest_df = load_interest_rate_monthly()
    if interest_df is not None:
        datasets.append(interest_df)
    
    nifty_df = load_nifty50_monthly()
    if nifty_df is not None:
        datasets.append(nifty_df)
    
    vix_df = load_vix_monthly()
    if vix_df is not None:
        datasets.append(vix_df)
    
    if len(datasets) == 0:
        print("\n❌ ERROR: No datasets loaded successfully!")
        return
    
    print(f"\n✅ Successfully loaded {len(datasets)} datasets")
    
    # ============================================================
    # STEP 2: MERGE DATASETS
    # ============================================================
    
    print("\n\n📥 STEP 2: Merging datasets...")
    print("-" * 60)
    
    merged_df = merge_datasets(datasets)
    
    # ============================================================
    # STEP 3: SORT BY TIME
    # ============================================================
    
    print("\n\n📥 STEP 3: Sorting by time...")
    print("-" * 60)
    
    sorted_df = sort_by_time(merged_df)
    
    # ============================================================
    # STEP 4: HANDLE MISSING VALUES (INTERPOLATION FIRST!)
    # ============================================================
    
    print("\n\n📥 STEP 4: Cleaning missing values...")
    print("-" * 60)
    
    cleaned_df = handle_missing_values(sorted_df)
    
    # ============================================================
    # STEP 5: FINALIZE DATA TYPES
    # ============================================================
    
    print("\n\n📥 STEP 5: Finalizing data types...")
    print("-" * 60)
    
    final_df = finalize_data_types(cleaned_df)
    
    # ============================================================
    # STEP 6: SAVE OUTPUT
    # ============================================================
    
    print("\n\n📥 STEP 6: Saving final dataset...")
    print("-" * 60)
    
    # Reorder columns for clarity
    column_order = ['Country', 'Year', 'Month', 'Inflation', 'InterestRate', 
                    'ExchangeRate', 'NIFTY50', 'VIX']
    
    available_columns = [col for col in column_order if col in final_df.columns]
    final_df = final_df[available_columns]
    
    # Save to CSV
    final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    print(f"\n✅ Final dataset saved → {OUTPUT_FILE}")
    
    # Display summary
    print("\n" + "="*70)
    print("📊 ECONOMY CLEANING SUMMARY (MONTHLY)")
    print("="*70)
    print(f"\n   Final rows: {len(final_df)}")
    print(f"   Countries: {final_df['Country'].nunique()}")
    print(f"   Time range: {final_df['Year'].min()}/{final_df['Month'].min()} - {final_df['Year'].max()}/{final_df['Month'].max()}")
    print(f"\n   Columns: {list(final_df.columns)}")
    print(f"\n   Data completeness:")
    
    for col in final_df.columns:
        if col not in ['Country', 'Year', 'Month']:
            non_null = final_df[col].notna().sum()
            pct = non_null / len(final_df) * 100
            print(f"      {col:20s}: {non_null:4d} ({pct:5.1f}%)")
    
    print("\n" + "="*70)
    print("✅ ECONOMY DATA CLEANING (MONTHLY) COMPLETED")
    print("="*70)


if __name__ == "__main__":
    main()
