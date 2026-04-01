"""
🧹 Trade Data Post-Cleaner

Final cleaning step for already merged trade dataset.
Removes invalid rows, standardizes data, and ensures production quality.

Input:  data/processed/trade/final_trade.csv
Output: data/processed/trade/cleaned_trade.csv
"""

import pandas as pd
import os

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

# Input/Output paths
BASE_PATH = os.path.abspath(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/final_trade.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/cleaned_trade.csv")

# Expected columns
EXPECTED_COLUMNS = ['Country', 'Partner', 'Year', 'Trade_Value', 'Trade_Type']


# ================================================================
# 🧼 DATA CLEANING FUNCTIONS
# ================================================================

def fix_country_column(df):
    """
    Fix Country column:
    - Remove NaN values
    - Remove numeric values (e.g., "500")
    - Strip spaces
    - Convert to title case
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n🌍 FIXING COUNTRY COLUMN")
    print(f"   Before: {len(df):,} rows")
    
    # Drop NaN countries
    before = len(df)
    df = df[df['Country'].notna()]
    after = len(df)
    print(f"   ✓ Dropped {before - after:,} rows with NaN countries")
    
    # Remove numeric countries (e.g., "500")
    before = len(df)
    df = df[~df['Country'].astype(str).str.isnumeric()]
    after = len(df)
    print(f"   ✓ Removed {before - after:,} rows with numeric countries")
    
    # Standardize country names
    df['Country'] = df['Country'].astype(str).str.strip().str.title()
    
    print(f"   After: {len(df):,} rows")
    print(f"   Unique countries: {df['Country'].nunique():,}")
    
    return df


def fix_partner_column(df):
    """
    Fix Partner column:
    - Replace NaN with "Global"
    - Replace empty strings with "Global"
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n🤝 FIXING PARTNER COLUMN")
    
    # Count missing partners before fix
    missing_partners = df['Partner'].isna().sum() + (df['Partner'] == '').sum()
    print(f"   Missing partners before fix: {missing_partners:,}")
    
    # Replace NaN with "Global"
    df['Partner'] = df['Partner'].fillna('Global')
    
    # Replace empty strings with "Global"
    df['Partner'] = df['Partner'].replace('', 'Global')
    
    # Strip spaces and title case
    df['Partner'] = df['Partner'].astype(str).str.strip().str.title()
    
    print(f"   ✓ All missing partners set to 'Global'")
    print(f"   Unique partners: {df['Partner'].nunique():,}")
    
    return df


def fix_year_column(df):
    """
    Fix Year column:
    - Convert to numeric
    - Drop invalid values
    - Convert to integer
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n📅 FIXING YEAR COLUMN")
    print(f"   Before: {len(df):,} rows")
    
    # Convert to numeric
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    
    # Drop invalid years
    before = len(df)
    df = df.dropna(subset=['Year'])
    after = len(df)
    print(f"   ✓ Dropped {before - after:,} rows with invalid years")
    
    # Convert to integer
    df['Year'] = df['Year'].astype(int)
    
    # Filter reasonable year range
    before = len(df)
    df = df[(df['Year'] >= 1900) & (df['Year'] <= 2100)]
    after = len(df)
    if before - after > 0:
        print(f"   ✓ Removed {before - after:,} rows outside 1900-2100 range")
    
    print(f"   After: {len(df):,} rows")
    print(f"   Year range: {df['Year'].min()} - {df['Year'].max()}")
    
    return df


def fix_trade_value_column(df):
    """
    Fix Trade_Value column:
    - Remove commas
    - Convert to numeric
    - Drop invalid values
    - Keep only positive values
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n💰 FIXING TRADE_VALUE COLUMN")
    print(f"   Before: {len(df):,} rows")
    
    # Remove commas from string representation
    df['Trade_Value'] = df['Trade_Value'].astype(str).str.replace(',', '', regex=False)
    
    # Convert to numeric
    df['Trade_Value'] = pd.to_numeric(df['Trade_Value'], errors='coerce')
    
    # Drop invalid values
    before = len(df)
    df = df.dropna(subset=['Trade_Value'])
    after = len(df)
    print(f"   ✓ Dropped {before - after:,} rows with invalid trade values")
    
    # Keep only positive values
    before = len(df)
    df = df[df['Trade_Value'] > 0]
    after = len(df)
    if before - after > 0:
        print(f"   ✓ Removed {before - after:,} rows with non-positive trade values")
    
    print(f"   After: {len(df):,} rows")
    print(f"   Total trade value: ${df['Trade_Value'].sum():,.2f}")
    
    return df


def fix_trade_type_column(df):
    """
    Fix Trade_Type column:
    - Keep only valid values ("Export" or "Import")
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n📊 FIXING TRADE_TYPE COLUMN")
    print(f"   Before: {len(df):,} rows")
    
    # Check unique values before filtering
    print(f"   Unique values before: {df['Trade_Type'].unique().tolist()}")
    
    # Keep only valid trade types
    before = len(df)
    df = df[df['Trade_Type'].isin(['Export', 'Import'])]
    after = len(df)
    print(f"   ✓ Removed {before - after:,} rows with invalid trade types")
    
    # Standardize to title case
    df['Trade_Type'] = df['Trade_Type'].str.strip().str.title()
    
    print(f"   After: {len(df):,} rows")
    print(f"   Export records: {(df['Trade_Type'] == 'Export').sum():,}")
    print(f"   Import records: {(df['Trade_Type'] == 'Import').sum():,}")
    
    return df


def remove_duplicates_and_sort(df):
    """
    Remove duplicates and sort dataset.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n🔄 REMOVING DUPLICATES AND SORTING")
    print(f"   Before: {len(df):,} rows")
    
    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=['Country', 'Partner', 'Year', 'Trade_Type'])
    after = len(df)
    print(f"   ✓ Removed {before - after:,} duplicate rows")
    
    # Sort by Country, then Year
    df = df.sort_values(['Country', 'Year']).reset_index(drop=True)
    
    print(f"   After: {len(df):,} rows")
    print(f"   ✓ Dataset sorted by Country and Year")
    
    return df


# ================================================================
# 💾 SAVE AND REPORT
# ================================================================

def save_cleaned_data(df, output_path):
    """
    Save cleaned dataset and print summary statistics.
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame
        output_path (str): Output file path
    """
    print("\n" + "="*70)
    print("💾 SAVING CLEANED DATASET")
    print("="*70)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ Saved to: {output_path}")
    print(f"   Final rows: {len(df):,}")
    print(f"   File size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
    
    # Print comprehensive summary
    print("\n" + "="*70)
    print("📊 FINAL SUMMARY STATISTICS")
    print("="*70)
    
    total_rows = len(df)
    unique_countries = df['Country'].nunique()
    unique_partners = df['Partner'].nunique()
    min_year = int(df['Year'].min())
    max_year = int(df['Year'].max())
    
    export_count = len(df[df['Trade_Type'] == 'Export'])
    import_count = len(df[df['Trade_Type'] == 'Import'])
    
    valid_values = df['Trade_Value'].dropna()
    total_value = valid_values.sum()
    avg_value = valid_values.mean()
    
    print(f"\n📈 Dataset Size:")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Unique countries: {unique_countries:,}")
    print(f"   Unique partners: {unique_partners:,}")
    
    print(f"\n📅 Temporal Coverage:")
    print(f"   Year range: {min_year} - {max_year}")
    print(f"   Years covered: {max_year - min_year + 1}")
    
    print(f"\n💰 Trade Values:")
    print(f"   Total trade value: ${total_value:,.2f}")
    print(f"   Average trade value: ${avg_value:,.2f}")
    
    print(f"\n📊 Trade Flow Distribution:")
    print(f"   - Exports: {export_count:,} records ({export_count/total_rows*100:.1f}%)")
    print(f"   - Imports: {import_count:,} records ({import_count/total_rows*100:.1f}%)")
    
    # Sample preview
    print("\n📋 SAMPLE DATA (First 10 rows):")
    print("-" * 70)
    print(df.head(10).to_string(index=False))
    
    # Top countries
    print("\n🌍 TOP 10 COUNTRIES BY RECORDS:")
    top_countries = df['Country'].value_counts().head(10)
    print(top_countries.to_string())
    
    # Quality metrics
    print("\n✅ DATA QUALITY METRICS:")
    print(f"   ✓ No NaN values in Country: {df['Country'].notna().all()}")
    print(f"   ✓ No NaN values in Partner: {df['Partner'].notna().all()}")
    print(f"   ✓ No NaN values in Year: {df['Year'].notna().all()}")
    print(f"   ✓ No NaN values in Trade_Value: {df['Trade_Value'].notna().all()}")
    print(f"   ✓ All Trade_Value > 0: {(df['Trade_Value'] > 0).all()}")
    print(f"   ✓ Valid Trade_Type values: {df['Trade_Type'].isin(['Export', 'Import']).all()}")
    print(f"   ✓ No duplicates: {df.duplicated(subset=['Country', 'Partner', 'Year', 'Trade_Type']).sum() == 0}")


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """
    Main execution function - applies all cleaning steps in order.
    """
    print("\n" + "="*70)
    print("🧹 TRADE DATA POST-CLEANER")
    print("="*70)
    
    # Load input file
    print(f"\n📂 Loading: {INPUT_FILE}")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: Input file not found at {INPUT_FILE}")
        print(f"   Please ensure final_trade.csv exists first!")
        return
    
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"   ✓ Loaded successfully")
        print(f"   Original rows: {len(df):,}")
        print(f"   Columns: {df.columns.tolist()}")
        
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return
    
    # Apply cleaning steps IN ORDER
    print("\n" + "="*70)
    print("🔧 APPLYING CLEANING STEPS")
    print("="*70)
    
    # Step 1: Fix Country column
    df = fix_country_column(df)
    
    # Step 2: Fix Partner column
    df = fix_partner_column(df)
    
    # Step 3: Fix Year column
    df = fix_year_column(df)
    
    # Step 4: Fix Trade_Value column
    df = fix_trade_value_column(df)
    
    # Step 5: Fix Trade_Type column
    df = fix_trade_type_column(df)
    
    # Step 6: Remove duplicates and sort
    df = remove_duplicates_and_sort(df)
    
    # Ensure correct column order
    df = df[EXPECTED_COLUMNS]
    
    # Save and report
    save_cleaned_data(df, OUTPUT_FILE)
    
    print("\n" + "="*70)
    print("🎉 TRADE DATA POST-CLEANING COMPLETE")
    print("="*70)
    print(f"\n✅ Input:  {INPUT_FILE}")
    print(f"✅ Output: {OUTPUT_FILE}")
    print(f"📁 Production-ready dataset!")
    print("="*70)


if __name__ == "__main__":
    main()
