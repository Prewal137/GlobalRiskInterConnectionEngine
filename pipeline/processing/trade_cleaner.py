"""
🧹 Trade Data Cleaner (Final Version)

Cleans and merges selected trade datasets into one unified dataset.

Input:  data/raw/trade/ (8 specific files)
Output: data/processed/trade/final_trade.csv

Final Format: Country | Partner | Year | Trade_Value | Trade_Type
"""

import pandas as pd
import os

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

# Files to process with EXACT mapping rules
SELECTED_FILES = [
    'sathwiksalian1515_17707384223090022.csv',  # Export dataset
    'sathwiksalian1515_17707384778729446.csv',  # Export destinations
    'sathwiksalian1515_1770739324737016.csv',   # Import dataset
    'sathwiksalian1515_17707394151529863.csv',  # Long time-series
    'sathwiksalian1515_17707394426811123.csv',  # Clean dataset
    'trade 1.csv',                                # Custom trade file
    'trade 2.csv',                                # Custom trade file
    'trade 3.csv'                                 # Custom trade file
]

# Standard output columns
OUTPUT_COLUMNS = ['Country', 'Partner', 'Year', 'Trade_Value', 'Trade_Type']

# Input/Output paths
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
TRADE_RAW_PATH = os.path.join(BASE_PATH, "data", "raw", "trade")
TRADE_PROCESSED_PATH = os.path.join(BASE_PATH, "data", "processed", "trade")


# ================================================================
# 🎯 FILE-SPECIFIC MAPPING FUNCTIONS
# ================================================================

def map_file_1(df):
    """
    sathwiksalian1515_17707384223090022.csv - Export dataset
    
    Mapping:
    Country -> Country
    Country Of Export -> Partner
    Export Value -> Trade_Value
    Year -> Year
    Trade_Type = "Export"
    """
    # Clean column names (remove carriage returns)
    df.columns = [col.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ') for col in df.columns]
    
    result = pd.DataFrame()
    result['Country'] = df['Country'].astype(str).str.strip()
    result['Partner'] = df['Country Of Export'].astype(str).str.strip()
    
    # Extract year from text like "Financial Year (Apr - Mar), 2024"
    result['Year'] = df['Year'].astype(str).str.extract(r'(\d{4})', expand=False)
    result['Year'] = pd.to_numeric(result['Year'], errors='coerce')
    
    # Clean value - remove commas and convert to numeric
    value_col = 'Export Value (UOM:INR(IndianRupees)), Scaling Factor:100000'
    result['Trade_Value'] = df[value_col].astype(str)
    result['Trade_Value'] = result['Trade_Value'].str.replace(',', '', regex=False)
    result['Trade_Value'] = pd.to_numeric(result['Trade_Value'], errors='coerce')
    
    result['Trade_Type'] = 'Export'
    
    return result


def map_file_2(df):
    """
    sathwiksalian1515_17707384778729446.csv - Export destinations
    
    Mapping:
    Country -> Country
    Export Destinations -> Partner
    Export Values -> Trade_Value
    Year -> Year
    Trade_Type = "Export"
    """
    # Clean column names
    df.columns = [col.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ') for col in df.columns]
    
    result = pd.DataFrame()
    result['Country'] = df['Country'].astype(str).str.strip()
    result['Partner'] = df['Export Destinations'].astype(str).str.strip()
    
    # Extract year
    result['Year'] = df['Year'].astype(str).str.extract(r'(\d{4})', expand=False)
    result['Year'] = pd.to_numeric(result['Year'], errors='coerce')
    
    # Clean value
    value_col = 'Export Values (UOM:USD(USDollar)), Scaling Factor:1000000000'
    result['Trade_Value'] = df[value_col].astype(str)
    result['Trade_Value'] = result['Trade_Value'].str.replace(',', '', regex=False)
    result['Trade_Value'] = pd.to_numeric(result['Trade_Value'], errors='coerce')
    
    result['Trade_Type'] = 'Export'
    
    return result


def map_file_3(df):
    """
    sathwiksalian1515_1770739324737016.csv - Import dataset
    
    Mapping:
    Country -> Country
    Country Of Import -> Partner
    Import Value -> Trade_Value
    Year -> Year
    Trade_Type = "Import"
    """
    # Clean column names
    df.columns = [col.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ') for col in df.columns]
    
    result = pd.DataFrame()
    result['Country'] = df['Country'].astype(str).str.strip()
    result['Partner'] = df['Country Of Import'].astype(str).str.strip()
    
    # Extract year
    result['Year'] = df['Year'].astype(str).str.extract(r'(\d{4})', expand=False)
    result['Year'] = pd.to_numeric(result['Year'], errors='coerce')
    
    # Clean value
    value_col = 'Import Value (UOM:INR(IndianRupees)), Scaling Factor:100000'
    result['Trade_Value'] = df[value_col].astype(str)
    result['Trade_Value'] = result['Trade_Value'].str.replace(',', '', regex=False)
    result['Trade_Value'] = pd.to_numeric(result['Trade_Value'], errors='coerce')
    
    result['Trade_Type'] = 'Import'
    
    return result


def map_file_4(df):
    """
    sathwiksalian1515_17707394151529863.csv - Long time-series
    
    Mapping:
    Country -> Country
    Export Country -> Partner
    Export Value -> Trade_Value
    Year -> Year
    Trade_Type = "Export"
    """
    # Clean column names
    df.columns = [col.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ') for col in df.columns]
    
    result = pd.DataFrame()
    result['Country'] = df['Country'].astype(str).str.strip()
    result['Partner'] = df['Export Country'].astype(str).str.strip()
    
    # Extract year
    result['Year'] = df['Year'].astype(str).str.extract(r'(\d{4})', expand=False)
    result['Year'] = pd.to_numeric(result['Year'], errors='coerce')
    
    # Clean value - try to find the correct column name
    value_cols = [
        'Export Value (UOM:INR(IndianRupees)), Scaling Factor:100000',
        'Export Value'
    ]
    
    value_col_found = None
    for vc in value_cols:
        if vc in df.columns:
            value_col_found = vc
            break
    
    if value_col_found:
        result['Trade_Value'] = df[value_col_found].astype(str)
        result['Trade_Value'] = result['Trade_Value'].str.replace(',', '', regex=False)
        result['Trade_Value'] = pd.to_numeric(result['Trade_Value'], errors='coerce')
    else:
        print(f"   ⚠️ Warning: Value column not found. Available: {df.columns.tolist()}")
        result['Trade_Value'] = None
    
    result['Trade_Type'] = 'Export'
    
    return result


def map_file_5(df):
    """
    sathwiksalian1515_17707394426811123.csv - Clean dataset
    
    Mapping:
    Country -> Country
    No partner column -> Set to "Global"
    Import Value -> Trade_Value
    Year -> Year
    Trade_Type = "Import"
    """
    # Clean column names
    df.columns = [col.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ') for col in df.columns]
    
    result = pd.DataFrame()
    result['Country'] = df['Country'].astype(str).str.strip()
    result['Partner'] = 'Global'  # No partner column
    result['Year'] = df['Year'].astype(str).str.extract(r'(\d{4})', expand=False)
    result['Year'] = pd.to_numeric(result['Year'], errors='coerce')
    
    # Clean value
    value_col = 'Import Value (UOM:INR(IndianRupees)), Scaling Factor:100000'
    result['Trade_Value'] = df[value_col].astype(str)
    result['Trade_Value'] = result['Trade_Value'].str.replace(',', '', regex=False)
    result['Trade_Value'] = pd.to_numeric(result['Trade_Value'], errors='coerce')
    
    result['Trade_Type'] = 'Import'
    
    return result


def detect_and_map_generic(df, filename):
    """
    Generic mapping for trade 1.csv, trade 2.csv, trade 3.csv
    
    Uses keyword-based column detection:
    Country → ["country"]
    Partner → ["partner", "destination"]
    Value → ["value", "trade"]
    Year → ["year"]
    
    If partner missing → Partner = "Global"
    """
    # Normalize column names (lowercase)
    df.columns = [col.lower().strip() for col in df.columns]
    
    result = pd.DataFrame()
    
    # Detect country column
    country_col = None
    for col in df.columns:
        if 'country' in col:
            country_col = col
            break
    
    if not country_col:
        print(f"   ⚠️ Warning: No country column found in {filename}")
        return None
    
    result['Country'] = df[country_col].astype(str).str.strip()
    
    # Detect partner column
    partner_col = None
    for col in df.columns:
        if 'partner' in col or 'destination' in col:
            partner_col = col
            break
    
    if partner_col:
        result['Partner'] = df[partner_col].astype(str).str.strip()
    else:
        print(f"   ℹ️ No partner column found in {filename}, setting to 'Global'")
        result['Partner'] = 'Global'
    
    # Detect year column
    year_col = None
    for col in df.columns:
        if 'year' in col:
            year_col = col
            break
    
    if year_col:
        # Extract 4-digit year from text
        result['Year'] = df[year_col].astype(str).str.extract(r'(\d{4})', expand=False)
        result['Year'] = pd.to_numeric(result['Year'], errors='coerce')
    else:
        print(f"   ⚠️ Warning: No year column found in {filename}")
        return None
    
    # Detect value column
    value_col = None
    for col in df.columns:
        if 'value' in col or 'trade' in col:
            value_col = col
            break
    
    if value_col:
        result['Trade_Value'] = df[value_col].astype(str).str.replace(',', '', regex=False)
        result['Trade_Value'] = pd.to_numeric(result['Trade_Value'], errors='coerce')
    else:
        print(f"   ⚠️ Warning: No value column found in {filename}")
        return None
    
    # Determine trade type from filename
    filename_lower = filename.lower()
    if 'import' in filename_lower:
        result['Trade_Type'] = 'Import'
    elif 'export' in filename_lower:
        result['Trade_Type'] = 'Export'
    else:
        # Default based on common patterns
        result['Trade_Type'] = 'Export'
    
    return result


# ================================================================
# 📂 PROCESSING FUNCTION
# ================================================================

def process_file(file_path):
    """
    Process a single file using appropriate mapping.
    
    Args:
        file_path (str): Path to the file
    
    Returns:
        pd.DataFrame or None: Cleaned DataFrame
    """
    filename = os.path.basename(file_path)
    print(f"\n🔄 Processing: {filename}")
    
    try:
        # Load file with encoding fallback and low_memory=False
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                print(f"   ✓ Loaded successfully ({encoding})")
                break
            except UnicodeDecodeError:
                continue
        else:
            print(f"   ❌ Could not read file with any encoding")
            return None
        
        original_rows = len(df)
        print(f"   📊 Original rows: {original_rows:,}")
        
        # Print columns for debugging
        print(f"   📋 Columns: {df.columns.tolist()[:5]}...")  # First 5 columns
        
        # Apply file-specific mapping
        if filename == 'sathwiksalian1515_17707384223090022.csv':
            cleaned_df = map_file_1(df)
        elif filename == 'sathwiksalian1515_17707384778729446.csv':
            cleaned_df = map_file_2(df)
        elif filename == 'sathwiksalian1515_1770739324737016.csv':
            cleaned_df = map_file_3(df)
        elif filename == 'sathwiksalian1515_17707394151529863.csv':
            cleaned_df = map_file_4(df)
        elif filename == 'sathwiksalian1515_17707394426811123.csv':
            cleaned_df = map_file_5(df)
        elif filename in ['trade 1.csv', 'trade 2.csv', 'trade 3.csv']:
            cleaned_df = detect_and_map_generic(df, filename)
        else:
            print(f"   ⚠️ Unknown file, using generic mapping")
            cleaned_df = detect_and_map_generic(df, filename)
        
        if cleaned_df is None:
            print(f"   ❌ Failed to process file")
            return None
        
        # Debug info
        print(f"   📊 After mapping:")
        print(f"      - Country nulls: {cleaned_df['Country'].isna().sum()}")
        print(f"      - Year nulls: {cleaned_df['Year'].isna().sum()}")
        print(f"      - Trade_Value nulls: {cleaned_df['Trade_Value'].isna().sum()}")
        print(f"      - Sample years: {cleaned_df['Year'].dropna().head(3).tolist()}")
        
        # Drop invalid rows
        before_drop = len(cleaned_df)
        cleaned_df = cleaned_df.dropna(subset=['Country', 'Year', 'Trade_Value'])
        after_drop = len(cleaned_df)
        
        dropped = before_drop - after_drop
        if dropped > 0:
            print(f"   🗑️ Dropped {dropped:,} invalid rows")
        
        print(f"   ✅ Valid rows: {after_drop:,}")
        
        return cleaned_df
        
    except Exception as e:
        print(f"   ❌ Error processing {filename}: {e}")
        import traceback
        traceback.print_exc()
        return None


# ================================================================
# 🔗 MAIN MERGE FUNCTION
# ================================================================

def merge_all_files():
    """
    Merge all selected trade datasets.
    
    Returns:
        pd.DataFrame: Combined dataset
    """
    print("\n" + "="*70)
    print("🧹 TRADE DATA CLEANER")
    print("="*70)
    
    # Ensure output directory exists
    os.makedirs(TRADE_PROCESSED_PATH, exist_ok=True)
    
    # Process each file
    all_dataframes = []
    
    for i, filename in enumerate(SELECTED_FILES, 1):
        print(f"\n[{i}/{len(SELECTED_FILES)}]", end=" ")
        file_path = os.path.join(TRADE_RAW_PATH, filename)
        
        if not os.path.exists(file_path):
            print(f"⚠️ File not found: {filename}")
            continue
        
        cleaned_df = process_file(file_path)
        
        if cleaned_df is not None and len(cleaned_df) > 0:
            all_dataframes.append(cleaned_df)
    
    if not all_dataframes:
        print("\n❌ No datasets were successfully processed!")
        return None
    
    # Combine all datasets
    print("\n" + "="*70)
    print("🔗 MERGING DATASETS")
    print("="*70)
    
    final_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"\n   📊 Total rows after merge: {len(final_df):,}")
    
    return final_df


# ================================================================
# 💾 SAVE AND REPORT
# ================================================================

def save_final_output(df, output_path):
    """
    Save final dataset and print summary.
    
    Args:
        df (pd.DataFrame): Final DataFrame
        output_path (str): Output file path
    """
    if df is None or len(df) == 0:
        print("\n⚠️ No data to save!")
        return
    
    print("\n" + "="*70)
    print("💾 SAVING FINAL DATASET")
    print("="*70)
    
    # Remove duplicates
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['Country', 'Partner', 'Year', 'Trade_Type'])
    after_dedup = len(df)
    
    removed = before_dedup - after_dedup
    if removed > 0:
        print(f"\n   🔄 Removed {removed:,} duplicate rows")
    
    # Sort by Country, then Year
    df = df.sort_values(['Country', 'Year']).reset_index(drop=True)
    
    # Ensure correct column order
    df = df[OUTPUT_COLUMNS]
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ Saved to: {output_path}")
    print(f"   Total rows: {len(df):,}")
    
    # Print summary statistics
    print("\n" + "="*70)
    print("📊 SUMMARY STATISTICS")
    print("="*70)
    
    total_rows = len(df)
    unique_countries = df['Country'].nunique()
    min_year = int(df['Year'].min())
    max_year = int(df['Year'].max())
    
    export_count = len(df[df['Trade_Type'] == 'Export'])
    import_count = len(df[df['Trade_Type'] == 'Import'])
    
    valid_values = df['Trade_Value'].dropna()
    total_value = valid_values.sum()
    
    print(f"\nTotal rows: {total_rows:,}")
    print(f"Unique countries: {unique_countries:,}")
    print(f"Year range: {min_year} - {max_year}")
    print(f"Total trade value: ${total_value:,.2f}")
    
    print(f"\n📈 Trade Flow:")
    print(f"   - Exports: {export_count:,} records")
    print(f"   - Imports: {import_count:,} records")
    
    # Sample preview
    print("\n📋 SAMPLE DATA (First 10 rows):")
    print("-" * 70)
    print(df.head(10).to_string(index=False))
    
    # Trade type distribution
    print("\n📊 TRADE TYPE DISTRIBUTION:")
    print(df['Trade_Type'].value_counts().to_string())
    
    # Top countries
    print("\n🌍 TOP 10 COUNTRIES BY RECORDS:")
    top_countries = df['Country'].value_counts().head(10)
    print(top_countries.to_string())


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """
    Main execution function.
    """
    print("\n🚀 Starting Trade Data Cleaning Pipeline...")
    print("="*70)
    
    # Merge all files
    merged_df = merge_all_files()
    
    # Define output path
    output_path = os.path.join(TRADE_PROCESSED_PATH, "final_trade.csv")
    
    # Save and report
    save_final_output(merged_df, output_path)
    
    print("\n" + "="*70)
    print("🎉 TRADE DATA CLEANING COMPLETE")
    print("="*70)
    print(f"\n✅ Output file: {output_path}")
    print(f"📁 Ready for analysis!")
    print("="*70)


if __name__ == "__main__":
    main()