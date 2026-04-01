"""
📊 Trade Feature Engineering Script

Transforms cleaned trade data into machine learning features.

Input:  data/processed/trade/cleaned_trade.csv
Output: data/processed/trade/trade_features.csv
"""

import pandas as pd
import os

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

# Input/Output paths
BASE_PATH = os.path.abspath(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/cleaned_trade.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/trade_features.csv")


# ================================================================
# 🎯 FEATURE ENGINEERING FUNCTIONS
# ================================================================

def load_and_prepare_data(input_path):
    """
    Load cleaned trade data and ensure correct data types.
    
    Args:
        input_path (str): Path to input file
    
    Returns:
        pd.DataFrame: Loaded DataFrame
    """
    print("\n" + "="*70)
    print("📂 STEP 1: LOAD DATA")
    print("="*70)
    
    df = pd.read_csv(input_path)
    
    # Ensure correct data types
    df['Country'] = df['Country'].astype(str)
    df['Partner'] = df['Partner'].astype(str)
    df['Year'] = df['Year'].astype(int)
    df['Trade_Value'] = df['Trade_Value'].astype(float)
    df['Trade_Type'] = df['Trade_Type'].astype(str)
    
    print(f"✓ Loaded {len(df):,} rows")
    print(f"✓ Columns: {df.columns.tolist()}")
    print(f"✓ Data types:\n{df.dtypes}")
    
    return df


def pivot_trade_data(df):
    """
    Pivot data to have Export and Import as separate columns.
    Aggregate by Country-Year, summing trade values.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Pivoted DataFrame
    """
    print("\n" + "="*70)
    print("📊 STEP 2: PIVOT DATA")
    print("="*70)
    
    # First, aggregate by Country, Year, Trade_Type (sum across all partners)
    aggregated = df.groupby(['Country', 'Year', 'Trade_Type'])['Trade_Value'].sum().reset_index()
    
    print(f"✓ Aggregated shape: {aggregated.shape}")
    print(f"✓ Sample:\n{aggregated.head(10)}")
    
    # Now pivot
    pivoted_df = aggregated.pivot_table(
        index=['Country', 'Year'],
        columns='Trade_Type',
        values='Trade_Value',
        aggfunc='sum'
    ).reset_index()
    
    # Ensure both Export and Import columns exist
    if 'Export' not in pivoted_df.columns:
        pivoted_df['Export'] = 0.0
    if 'Import' not in pivoted_df.columns:
        pivoted_df['Import'] = 0.0
    
    print(f"✓ Pivoted shape: {pivoted_df.shape}")
    print(f"✓ Columns: {pivoted_df.columns.tolist()}")
    print(f"✓ Sample:\n{pivoted_df.head()}")
    
    return pivoted_df


def handle_missing_values(df):
    """
    Fill missing Export and Import values with 0.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n" + "="*70)
    print("🔧 STEP 3: HANDLE MISSING VALUES")
    print("="*70)
    
    # Count missing before
    missing_export = df['Export'].isna().sum()
    missing_import = df['Import'].isna().sum()
    
    print(f"Missing Export values: {missing_export:,}")
    print(f"Missing Import values: {missing_import:,}")
    
    # Fill missing with 0
    df['Export'] = df['Export'].fillna(0.0)
    df['Import'] = df['Import'].fillna(0.0)
    
    # Verify
    print(f"✓ After filling: Export={df['Export'].isna().sum()}, Import={df['Import'].isna().sum()}")
    
    return df


def create_basic_features(df):
    """
    Create basic derived features.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: DataFrame with new features
    """
    print("\n" + "="*70)
    print("🔧 STEP 4: CREATE BASIC FEATURES")
    print("="*70)
    
    # Trade Balance = Export - Import
    df['Trade_Balance'] = df['Export'] - df['Import']
    print("✓ Created Trade_Balance")
    
    # Total Trade = Export + Import
    df['Total_Trade'] = df['Export'] + df['Import']
    print("✓ Created Total_Trade")
    
    # Show statistics
    print(f"\n📊 Trade Balance Statistics:")
    print(f"   Min: ${df['Trade_Balance'].min():,.2f}")
    print(f"   Max: ${df['Trade_Balance'].max():,.2f}")
    print(f"   Mean: ${df['Trade_Balance'].mean():,.2f}")
    
    print(f"\n📊 Total Trade Statistics:")
    print(f"   Min: ${df['Total_Trade'].min():,.2f}")
    print(f"   Max: ${df['Total_Trade'].max():,.2f}")
    print(f"   Mean: ${df['Total_Trade'].mean():,.2f}")
    
    return df


def sort_data(df):
    """
    Sort data by Country and Year.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Sorted DataFrame
    """
    print("\n" + "="*70)
    print("🔧 STEP 5: SORT DATA")
    print("="*70)
    
    # Sort by Country, then Year
    df = df.sort_values(['Country', 'Year']).reset_index(drop=True)
    
    print(f"✓ Sorted by Country and Year")
    print(f"✓ Shape: {df.shape}")
    
    return df


def create_country_features(df):
    """
    Create time-series features grouped by country.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: DataFrame with country-level features
    """
    print("\n" + "="*70)
    print("🔧 STEP 6: CREATE COUNTRY-LEVEL FEATURES")
    print("="*70)
    
    # Group by Country for time-series calculations
    print("\n📈 Calculating per-country features...")
    
    # Growth Rate: pct_change of Total_Trade
    df['Growth'] = df.groupby('Country')['Total_Trade'].pct_change()
    print("✓ Created Growth Rate (pct_change)")
    
    # Rolling Mean (window=3)
    df['Rolling_Mean_3'] = df.groupby('Country')['Total_Trade'].transform(
        lambda x: x.rolling(window=3).mean()
    )
    print("✓ Created Rolling Mean (window=3)")
    
    # Volatility (rolling std, window=3)
    df['Volatility_3'] = df.groupby('Country')['Total_Trade'].transform(
        lambda x: x.rolling(window=3).std()
    )
    print("✓ Created Volatility (rolling std, window=3)")
    
    # Export Growth Rate
    df['Export_Growth'] = df.groupby('Country')['Export'].pct_change()
    print("✓ Created Export Growth Rate")
    
    # Import Growth Rate
    df['Import_Growth'] = df.groupby('Country')['Import'].pct_change()
    print("✓ Created Import Growth Rate")
    
    # Export Share
    df['Export_Share'] = df['Export'] / df['Total_Trade'].replace(0, 1)
    print("✓ Created Export Share")
    
    # Import Share
    df['Import_Share'] = df['Import'] / df['Total_Trade'].replace(0, 1)
    print("✓ Created Import Share")
    
    # Trade Balance Ratio
    df['Balance_Ratio'] = df['Trade_Balance'] / df['Total_Trade'].replace(0, 1)
    print("✓ Created Trade Balance Ratio")
    
    # Show sample with new features
    print(f"\n📊 Sample Features (India):")
    india_sample = df[df['Country'] == 'India'].head(10)
    print(india_sample[['Country', 'Year', 'Total_Trade', 'Growth', 
                        'Rolling_Mean_3', 'Volatility_3']].to_string(index=False))
    
    return df


def detect_shocks(df):
    """
    Detect economic shocks based on negative growth.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: DataFrame with shock indicator
    """
    print("\n" + "="*70)
    print("🔧 STEP 7: DETECT ECONOMIC SHOCKS")
    print("="*70)
    
    # Shock = 1 if Growth < -0.3 else 0
    df['Shock'] = (df['Growth'] < -0.3).astype(int)
    print("✓ Created Shock indicator (Growth < -30%)")
    
    # Count shocks
    shock_count = df['Shock'].sum()
    total_rows = len(df)
    shock_percent = (shock_count / total_rows) * 100
    
    print(f"\n📊 Shock Statistics:")
    print(f"   Total shocks detected: {shock_count:,}")
    print(f"   Percentage of records: {shock_percent:.2f}%")
    
    # Show countries with most shocks
    if shock_count > 0:
        shock_countries = df[df['Shock'] == 1]['Country'].value_counts().head(10)
        print(f"\n📊 Top 10 Countries with Most Shocks:")
        for country, count in shock_countries.items():
            print(f"   {country}: {count} shocks")
    
    return df


def clean_final_data(df):
    """
    Clean final dataset - keep records even if they only have Export OR Import.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n" + "="*70)
    print("🔧 STEP 8: CLEAN FINAL DATA")
    print("="*70)
    
    # Count NaN before
    nan_before = df.isna().sum().sum()
    print(f"Total NaN values before cleaning: {nan_before:,}")
    
    # Only drop rows where Country or Year is NaN (should not happen)
    # Keep rows even if they only have Export OR Import
    df_clean = df.dropna(subset=['Country', 'Year']).reset_index(drop=True)
    
    # For growth calculations, we need at least Total_Trade
    df_clean = df_clean[df_clean['Total_Trade'].notna()].reset_index(drop=True)
    
    # Count NaN after
    nan_after = df_clean.isna().sum().sum()
    print(f"Total NaN values after cleaning: {nan_after}")
    
    # Rows removed
    rows_removed = len(df) - len(df_clean)
    print(f"\n✓ Removed {rows_removed:,} rows missing critical data")
    print(f"✓ Final shape: {df_clean.shape}")
    
    # Show how many have both vs only one
    has_both = ((df_clean['Export'] > 0) & (df_clean['Import'] > 0)).sum()
    has_only_export = ((df_clean['Export'] > 0) & (df_clean['Import'] == 0)).sum()
    has_only_import = ((df_clean['Export'] == 0) & (df_clean['Import'] > 0)).sum()
    
    print(f"\n📊 Data Coverage:")
    print(f"   Countries with BOTH Export & Import: {has_both:,}")
    print(f"   Countries with ONLY Export: {has_only_export:,}")
    print(f"   Countries with ONLY Import: {has_only_import:,}")
    
    return df_clean


def save_features(df, output_path):
    """
    Save feature-engineered dataset.
    
    Args:
        df (pd.DataFrame): Final DataFrame
        output_path (str): Output file path
    """
    print("\n" + "="*70)
    print("💾 STEP 9: SAVE OUTPUT")
    print("="*70)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ Saved to: {output_path}")
    print(f"   File size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
    print(f"   Total rows: {len(df):,}")
    print(f"   Total columns: {len(df.columns)}")
    
    # List all columns
    print(f"\n📋 All Features ({len(df.columns)} columns):")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i}. {col}")


def print_summary_statistics(df):
    """
    Print comprehensive summary statistics.
    
    Args:
        df (pd.DataFrame): Final DataFrame
    """
    print("\n" + "="*70)
    print("📊 STEP 10: SUMMARY STATISTICS")
    print("="*70)
    
    # Basic stats
    total_rows = len(df)
    unique_countries = df['Country'].nunique()
    min_year = int(df['Year'].min())
    max_year = int(df['Year'].max())
    
    print(f"\n📈 Dataset Size:")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Unique countries: {unique_countries:,}")
    print(f"   Year range: {min_year} - {max_year}")
    print(f"   Years covered: {max_year - min_year + 1}")
    
    # Growth statistics
    mean_growth = df['Growth'].mean()
    median_growth = df['Growth'].median()
    std_growth = df['Growth'].std()
    
    print(f"\n📊 Growth Rate Statistics:")
    print(f"   Mean growth: {mean_growth:.4f} ({mean_growth*100:.2f}%)")
    print(f"   Median growth: {median_growth:.4f} ({median_growth*100:.2f}%)")
    print(f"   Std deviation: {std_growth:.4f} ({std_growth*100:.2f}%)")
    
    # Shock statistics
    shock_count = df['Shock'].sum()
    shock_percent = (shock_count / total_rows) * 100
    
    print(f"\n⚡ Shock Detection:")
    print(f"   Number of shocks: {shock_count:,}")
    print(f"   Percentage: {shock_percent:.2f}%")
    
    # Trade statistics
    total_trade_sum = df['Total_Trade'].sum()
    avg_trade = df['Total_Trade'].mean()
    
    print(f"\n💰 Trade Statistics:")
    print(f"   Total trade volume: ${total_trade_sum:,.2f}")
    print(f"   Average trade per record: ${avg_trade:,.2f}")
    
    # Feature quality check
    print(f"\n✅ Feature Quality Check:")
    print(f"   Missing values: {df.isna().sum().sum()}")
    print(f"   Duplicate rows: {df.duplicated().sum()}")
    
    # Show final sample
    print(f"\n📋 Sample Data (First 10 rows):")
    print("-" * 70)
    print(df.head(10).to_string(index=False))


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """
    Main feature engineering pipeline.
    """
    print("\n" + "="*70)
    print("🚀 TRADE FEATURE ENGINEERING")
    print("="*70)
    
    # Step 1: Load data
    df = load_and_prepare_data(INPUT_FILE)
    
    # Step 2: Pivot data
    df = pivot_trade_data(df)
    
    # Step 3: Handle missing values
    df = handle_missing_values(df)
    
    # Step 4: Create basic features
    df = create_basic_features(df)
    
    # Step 5: Sort data
    df = sort_data(df)
    
    # Step 6: Create country-level features
    df = create_country_features(df)
    
    # Step 7: Detect shocks
    df = detect_shocks(df)
    
    # Step 8: Clean final data
    df = clean_final_data(df)
    
    # Step 9: Save output
    save_features(df, OUTPUT_FILE)
    
    # Step 10: Print summary
    print_summary_statistics(df)
    
    print("\n" + "="*70)
    print("🎉 FEATURE ENGINEERING COMPLETE")
    print("="*70)
    print(f"\n✅ Input:  {INPUT_FILE}")
    print(f"✅ Output: {OUTPUT_FILE}")
    print(f"📁 Ready for machine learning!")
    print("="*70)


if __name__ == "__main__":
    main()
