"""
🧹 Economy Sector Data Cleaner (Phase 3)

This script cleans and unifies economy datasets into a single final_economy.csv file.

Input: data/raw/economy/
Output: data/processed/economy/final_economy.csv

Selected Sources:
- Inflation (CPI)
- Interest Rate
- Exchange Rate
- Policy Uncertainty
- NIFTY 50 (Stock Market)
- VIX (Volatility Index)
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
OUTPUT_FILE = os.path.join(PROCESSED_ECONOMY_PATH, "final_economy.csv")

# Ensure output directory exists
os.makedirs(PROCESSED_ECONOMY_PATH, exist_ok=True)

# ================================================================
# 🔧 DATA LOADING FUNCTIONS
# ================================================================

def load_exchange_rate():
    """
    Load exchange rate data (DEXINUS - INR/USD).
    Aggregates daily data to yearly mean.
    
    Returns:
        pd.DataFrame: Country-Year level exchange rate data
    """
    print("\n💱 Loading Exchange Rate (DEXINUS)...")
    
    try:
        df = pd.read_csv(os.path.join(RAW_ECONOMY_PATH, "DEXINUS.csv"))
        
        # Convert observation_date to datetime
        df['observation_date'] = pd.to_datetime(df['observation_date'])
        df['Year'] = df['observation_date'].dt.year
        
        # Aggregate to yearly mean
        yearly_df = df.groupby('Year')['DEXINUS'].mean().reset_index()
        yearly_df.columns = ['Year', 'ExchangeRate']
        
        # Add country column
        yearly_df['Country'] = 'IND'
        
        print(f"   ✅ Loaded {len(yearly_df)} years ({yearly_df['Year'].min()}-{yearly_df['Year'].max()})")
        print(f"   Mean Exchange Rate: {yearly_df['ExchangeRate'].mean():.4f}")
        
        return yearly_df[['Country', 'Year', 'ExchangeRate']]
        
    except Exception as e:
        print(f"   ❌ Error loading exchange rate: {e}")
        return None


def load_inflation_cpi():
    """
    Load inflation data from CPI (Consumer Price Index).
    Calculates inflation rate as YoY change in CPI.
    
    Returns:
        pd.DataFrame: Country-Year level inflation data
    """
    print("\n📈 Loading Inflation (CPI)...")
    
    try:
        df = pd.read_csv(os.path.join(RAW_ECONOMY_PATH, "INDCPIALLMINMEI.csv"))
        
        # Convert observation_date to datetime
        df['observation_date'] = pd.to_datetime(df['observation_date'])
        df['Year'] = df['observation_date'].dt.year
        
        # Aggregate CPI to yearly mean first
        yearly_cpi = df.groupby('Year')['INDCPIALLMINMEI'].mean().reset_index()
        
        # Calculate inflation as year-over-year percentage change
        yearly_cpi['Inflation'] = yearly_cpi['INDCPIALLMINMEI'].pct_change() * 100
        
        # Drop first row (no previous year for comparison)
        yearly_cpi = yearly_cpi.dropna(subset=['Inflation'])
        
        yearly_cpi.columns = ['Year', 'CPI_Level', 'Inflation']
        yearly_cpi['Country'] = 'IND'
        
        print(f"   ✅ Loaded {len(yearly_cpi)} years ({yearly_cpi['Year'].min()}-{yearly_cpi['Year'].max()})")
        print(f"   Mean Inflation: {yearly_cpi['Inflation'].mean():.2f}%")
        
        return yearly_cpi[['Country', 'Year', 'Inflation']]
        
    except Exception as e:
        print(f"   ❌ Error loading inflation: {e}")
        return None


def load_interest_rate():
    """
    Load interest rate data (long-term government bond rates).
    
    Returns:
        pd.DataFrame: Country-Year level interest rate data
    """
    print("\n🏦 Loading Interest Rate...")
    
    try:
        # Use the main interest rate file
        df = pd.read_csv(os.path.join(RAW_ECONOMY_PATH, "INDIRLTLT01STM.csv"))
        
        # Convert observation_date to datetime
        df['observation_date'] = pd.to_datetime(df['observation_date'])
        df['Year'] = df['observation_date'].dt.year
        
        # Aggregate to yearly mean
        yearly_df = df.groupby('Year')['INDIRLTLT01STM'].mean().reset_index()
        yearly_df.columns = ['Year', 'InterestRate']
        
        yearly_df['Country'] = 'IND'
        
        print(f"   ✅ Loaded {len(yearly_df)} years ({yearly_df['Year'].min()}-{yearly_df['Year'].max()})")
        print(f"   Mean Interest Rate: {yearly_df['InterestRate'].mean():.2f}%")
        
        return yearly_df[['Country', 'Year', 'InterestRate']]
        
    except Exception as e:
        print(f"   ❌ Error loading interest rate: {e}")
        return None


def load_policy_uncertainty():
    """
    Load policy uncertainty index from WUI (World Uncertainty Index) data.
    
    Returns:
        pd.DataFrame: Country-Year level policy uncertainty data
    """
    print("\n📊 Loading Policy Uncertainty...")
    
    try:
        # Load WUI data
        df = pd.read_excel(os.path.join(RAW_ECONOMY_PATH, "WUI_Data.xlsx"))
        
        # Check column structure
        print(f"   Columns found: {list(df.columns)}")
        
        # The file has India News-Based Policy Uncertainty Index
        # Reshape if needed
        if 'India News-Based Policy Uncertainty Index' in df.columns:
            if 'Year' in df.columns and 'Month' in df.columns:
                # Melt to long format if wide
                df_melted = df.melt(
                    id_vars=['Year', 'Month'],
                    value_vars=['India News-Based Policy Uncertainty Index'],
                    var_name='Indicator',
                    value_name='PolicyUncertainty'
                )
                
                # Aggregate to yearly
                yearly_df = df_melted.groupby('Year')['PolicyUncertainty'].mean().reset_index()
                yearly_df['Country'] = 'IND'
                
                print(f"   ✅ Loaded {len(yearly_df)} years")
                print(f"   Mean Policy Uncertainty: {yearly_df['PolicyUncertainty'].mean():.2f}")
                
                return yearly_df[['Country', 'Year', 'PolicyUncertainty']]
        
        # Fallback: try to use available columns
        print("   ⚠️  Using alternative column structure...")
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            # Assume first numeric column is the index
            value_col = numeric_cols[0]
            
            # Try to extract year from data
            if 'Unnamed' in str(df.columns[0]):
                # Data might be transposed
                df_transposed = df.T
                df_transposed.columns = df_transposed.iloc[0]
                df_transposed = df_transposed.drop(df_transposed.index[0])
                
                # Reset index and use as year
                df_transposed['Year'] = df_transposed.index.astype(int)
                
                yearly_df = df_transposed.groupby('Year')[value_col].mean().reset_index()
                yearly_df.columns = ['Year', 'PolicyUncertainty']
                yearly_df['Country'] = 'IND'
                
                return yearly_df[['Country', 'Year', 'PolicyUncertainty']]
        
        return None
        
    except Exception as e:
        print(f"   ❌ Error loading policy uncertainty: {e}")
        return None


def load_nifty50():
    """
    Load NIFTY 50 stock market index data.
    Aggregates daily closing prices to yearly mean.
    
    Returns:
        pd.DataFrame: Country-Year level stock market data
    """
    print("\n📈 Loading NIFTY 50...")
    
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
                
                # Standardize column names
                df.columns = df.columns.str.strip()
                
                # Convert Date column
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
                    df['Year'] = df['Date'].dt.year
                    
                    # Use Close price
                    if 'Close' in df.columns:
                        yearly_close = df.groupby('Year')['Close'].mean().reset_index()
                        yearly_close.columns = ['Year', 'NIFTY50']
                        all_data.append(yearly_close)
                        
            except Exception as e:
                print(f"   ⚠️  Error reading {os.path.basename(file_path)}: {e}")
                continue
        
        if len(all_data) == 0:
            print("   ❌ No valid NIFTY data extracted")
            return None
        
        # Combine all years
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates by taking mean
        yearly_df = combined_df.groupby('Year')['NIFTY50'].mean().reset_index()
        yearly_df['Country'] = 'IND'
        
        print(f"   ✅ Loaded {len(yearly_df)} years ({yearly_df['Year'].min()}-{yearly_df['Year'].max()})")
        print(f"   Mean NIFTY 50: {yearly_df['NIFTY50'].mean():.2f}")
        
        return yearly_df[['Country', 'Year', 'NIFTY50']]
        
    except Exception as e:
        print(f"   ❌ Error loading NIFTY 50: {e}")
        return None


def load_vix():
    """
    Load India VIX (Volatility Index) data.
    Aggregates daily data to yearly mean.
    
    Returns:
        pd.DataFrame: Country-Year level volatility data
    """
    print("\n📉 Loading VIX (Volatility Index)...")
    
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
                
                # Standardize column names
                df.columns = df.columns.str.strip()
                
                # Convert Date column
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
                    df['Year'] = df['Date'].dt.year
                    
                    # Use Close price as VIX level
                    if 'Close' in df.columns:
                        yearly_vix = df.groupby('Year')['Close'].mean().reset_index()
                        yearly_vix.columns = ['Year', 'VIX']
                        all_data.append(yearly_vix)
                        
            except Exception as e:
                print(f"   ⚠️  Error reading {os.path.basename(file_path)}: {e}")
                continue
        
        if len(all_data) == 0:
            print("   ❌ No valid VIX data extracted")
            return None
        
        # Combine all years
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates by taking mean
        yearly_df = combined_df.groupby('Year')['VIX'].mean().reset_index()
        yearly_df['Country'] = 'IND'
        
        print(f"   ✅ Loaded {len(yearly_df)} years ({yearly_df['Year'].min()}-{yearly_df['Year'].max()})")
        print(f"   Mean VIX: {yearly_df['VIX'].mean():.2f}")
        
        return yearly_df[['Country', 'Year', 'VIX']]
        
    except Exception as e:
        print(f"   ❌ Error loading VIX: {e}")
        return None


# ================================================================
# 🔧 MERGING AND CLEANING
# ================================================================

def merge_datasets(datasets):
    """
    Merge all datasets on Country + Year.
    
    Args:
        datasets (list): List of DataFrames to merge
        
    Returns:
        pd.DataFrame: Merged dataset
    """
    print("\n🔗 Merging datasets...")
    
    # Start with base dataset (use the one with most years)
    base_df = max(datasets, key=len)
    print(f"   Base dataset: {len(base_df)} rows")
    
    # Merge all other datasets
    merged_df = base_df.copy()
    
    for i, df in enumerate(datasets):
        if df is not base_df:
            # Get feature column name (not Country or Year)
            feature_col = [col for col in df.columns if col not in ['Country', 'Year']][0]
            
            merged_df = pd.merge(
                merged_df,
                df[['Country', 'Year', feature_col]],
                on=['Country', 'Year'],
                how='outer'
            )
            
            print(f"   Merged {feature_col}: {merged_df[feature_col].notna().sum()} non-null values")
    
    print(f"\n   Total after merge: {len(merged_df)} rows")
    
    return merged_df


def handle_missing_values(df):
    """
    Handle missing values using forward fill and interpolation.
    
    Args:
        df (pd.DataFrame): Dataset with potential missing values
        
    Returns:
        pd.DataFrame: Cleaned dataset
    """
    print("\n🔧 Handling missing values...")
    
    # Sort by Country and Year
    df = df.sort_values(['Country', 'Year']).reset_index(drop=True)
    
    # For each country separately
    countries = df['Country'].unique()
    
    cleaned_dfs = []
    
    for country in countries:
        country_df = df[df['Country'] == country].copy()
        
        # Forward fill first (carry last known value forward)
        country_df = country_df.ffill()
        
        # Then backward fill (for leading NaNs)
        country_df = country_df.bfill()
        
        # Interpolate remaining gaps
        numeric_cols = country_df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if country_df[col].isna().any():
                country_df[col] = country_df[col].interpolate(method='linear')
        
        cleaned_dfs.append(country_df)
    
    result_df = pd.concat(cleaned_dfs, ignore_index=True)
    
    # Report missing values
    missing_before = df.isna().sum().sum()
    missing_after = result_df.isna().sum().sum()
    
    print(f"   Missing values before: {missing_before}")
    print(f"   Missing values after: {missing_after}")
    
    # Drop rows that are still too empty (more than 50% features missing)
    feature_cols = [col for col in result_df.columns if col not in ['Country', 'Year']]
    min_features = len(feature_cols) // 2
    
    mask = result_df[feature_cols].notna().sum(axis=1) >= min_features
    result_df = result_df[mask].reset_index(drop=True)
    
    print(f"   Rows after dropping sparse data: {len(result_df)}")
    
    return result_df


def finalize_data_types(df):
    """
    Ensure correct data types.
    
    Args:
        df (pd.DataFrame): Dataset
        
    Returns:
        pd.DataFrame: Dataset with proper types
    """
    print("\n🔧 Finalizing data types...")
    
    # Year as integer
    df['Year'] = df['Year'].astype(int)
    
    # All features as float
    feature_cols = [col for col in df.columns if col not in ['Country', 'Year']]
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
    print("🧹 ECONOMY SECTOR DATA CLEANING (PHASE 3)")
    print("="*70)
    
    # ============================================================
    # STEP 1: LOAD ALL DATASETS
    # ============================================================
    
    print("\n📥 STEP 1: Loading individual datasets...")
    print("-" * 60)
    
    datasets = []
    
    # Load each dataset
    exchange_df = load_exchange_rate()
    if exchange_df is not None:
        datasets.append(exchange_df)
    
    inflation_df = load_inflation_cpi()
    if inflation_df is not None:
        datasets.append(inflation_df)
    
    interest_df = load_interest_rate()
    if interest_df is not None:
        datasets.append(interest_df)
    
    policy_df = load_policy_uncertainty()
    if policy_df is not None:
        datasets.append(policy_df)
    
    nifty_df = load_nifty50()
    if nifty_df is not None:
        datasets.append(nifty_df)
    
    vix_df = load_vix()
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
    # STEP 3: HANDLE MISSING VALUES
    # ============================================================
    
    print("\n\n📥 STEP 3: Cleaning missing values...")
    print("-" * 60)
    
    cleaned_df = handle_missing_values(merged_df)
    
    # ============================================================
    # STEP 4: FINALIZE DATA TYPES
    # ============================================================
    
    print("\n\n📥 STEP 4: Finalizing data types...")
    print("-" * 60)
    
    final_df = finalize_data_types(cleaned_df)
    
    # ============================================================
    # STEP 5: SAVE OUTPUT
    # ============================================================
    
    print("\n\n📥 STEP 5: Saving final dataset...")
    print("-" * 60)
    
    # Reorder columns for clarity
    column_order = ['Country', 'Year', 'Inflation', 'InterestRate', 'ExchangeRate', 
                    'PolicyUncertainty', 'NIFTY50', 'VIX']
    
    available_columns = [col for col in column_order if col in final_df.columns]
    final_df = final_df[available_columns]
    
    # Save to CSV
    final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    print(f"\n✅ Final dataset saved → {OUTPUT_FILE}")
    
    # Display summary
    print("\n" + "="*70)
    print("📊 ECONOMY CLEANING SUMMARY")
    print("="*70)
    print(f"\n   Final rows: {len(final_df)}")
    print(f"   Countries: {final_df['Country'].nunique()}")
    print(f"   Year range: {final_df['Year'].min()} - {final_df['Year'].max()}")
    print(f"\n   Columns: {list(final_df.columns)}")
    print(f"\n   Data completeness:")
    
    for col in final_df.columns:
        if col not in ['Country', 'Year']:
            non_null = final_df[col].notna().sum()
            pct = non_null / len(final_df) * 100
            print(f"      {col:20s}: {non_null:4d} ({pct:5.1f}%)")
    
    print("\n" + "="*70)
    print("✅ ECONOMY DATA CLEANING COMPLETED")
    print("="*70)


if __name__ == "__main__":
    main()
