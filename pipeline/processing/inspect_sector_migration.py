"""
🔍 Migration Sector Data Inspection

Scans all migration datasets in data/raw/migration/ and extracts metadata.

Output:
    - data/processed/migration/migration_summary.csv

Extracts:
    - file_name
    - file_type (csv/xlsx/xls)
    - columns
    - time_column (detected)
    - min_year
    - max_year
    - row_count
"""

import pandas as pd
import os
import glob
from datetime import datetime

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
RAW_MIGRATION_PATH = os.path.join(BASE_PATH, "data", "raw", "migration")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "migration", "migration_summary.csv")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def get_migration_data_files(path):
    """
    Filter migration data files from World Bank downloads.
    
    Rules:
    - Include: Files starting with "API_" and ending with ".csv"
    - Exclude: Files starting with "Metadata_"
    
    Args:
        path (str): Path to migration data directory
        
    Returns:
        tuple: (valid_files, metadata_files) - lists of file paths
    """
    if not os.path.exists(path):
        print(f"❌ Directory not found: {path}")
        return [], []
    
    all_files = os.listdir(path)
    
    valid_files = []
    metadata_files = []
    
    for filename in sorted(all_files):
        filepath = os.path.join(path, filename)
        
        # Skip directories
        if not os.path.isfile(filepath):
            continue
        
        # Check if it's a metadata file (ignore)
        if filename.startswith("Metadata_"):
            metadata_files.append(filename)
            continue
        
        # Check if it's a valid API data file
        if filename.startswith("API_") and filename.endswith(".csv"):
            valid_files.append(filepath)
    
    return valid_files, metadata_files


def detect_time_column(columns):
    """
    Detect time-related column from list of columns.
    
    Priority:
    1. 'year' (exact match, case-insensitive)
    2. 'date' (exact match, case-insensitive)
    3. Columns containing 'year' or 'date'
    
    Args:
        columns (list): List of column names
        
    Returns:
        str or None: Detected time column name
    """
    # Convert to lowercase for matching
    cols_lower = [col.lower().strip() for col in columns]
    
    # Exact matches (highest priority)
    if 'year' in cols_lower:
        idx = cols_lower.index('year')
        return columns[idx]
    if 'date' in cols_lower:
        idx = cols_lower.index('date')
        return columns[idx]
    
    # Partial matches
    for i, col_lower in enumerate(cols_lower):
        if 'year' in col_lower or 'date' in col_lower:
            return columns[i]
    
    return None


def extract_year_range(df, time_col):
    """
    Extract min and max year from time column or year columns.
    
    Handles two formats:
    1. Long format: Single 'year' column with values
    2. Wide format (World Bank): Year columns like '1960', '1961', etc.
    
    Args:
        df (pd.DataFrame): DataFrame
        time_col (str): Time column name (or None for wide format)
        
    Returns:
        tuple: (min_year, max_year) or (None, None)
    """
    try:
        # Format 1: Traditional long format with year column
        if time_col and time_col != 'NOT DETECTED':
            # Try to convert to numeric (for year columns)
            years = pd.to_numeric(df[time_col], errors='coerce').dropna()
            
            if len(years) > 0:
                min_year = int(years.min())
                max_year = int(years.max())
                return min_year, max_year
            
            # If not numeric, try datetime parsing
            dates = pd.to_datetime(df[time_col], errors='coerce').dropna()
            
            if len(dates) > 0:
                min_year = int(dates.min().year)
                max_year = int(dates.max().year)
                return min_year, max_year
        
        # Format 2: World Bank wide format (year columns)
        # Look for columns that are 4-digit years
        year_columns = []
        for col in df.columns:
            col_str = str(col).strip()
            # Check if column name is a 4-digit year (1900-2099)
            if col_str.isdigit() and len(col_str) == 4:
                year_val = int(col_str)
                if 1900 <= year_val <= 2099:
                    year_columns.append(year_val)
        
        if year_columns:
            min_year = min(year_columns)
            max_year = max(year_columns)
            return min_year, max_year
        
    except Exception as e:
        print(f"      ⚠️  Could not extract year range: {e}")
    
    return None, None


def read_file(filepath):
    """
    Read CSV or Excel file.
    
    Args:
        filepath (str): Path to file
        
    Returns:
        pd.DataFrame or None: DataFrame if successful
    """
    ext = os.path.splitext(filepath)[1].lower()
    
    try:
        if ext == '.csv':
            # Try multiple parsing strategies for World Bank data
            # Strategy 1: Standard CSV
            try:
                df = pd.read_csv(filepath, nrows=100)
                return df
            except Exception:
                pass
            
            # Strategy 2: Skip metadata rows (World Bank often has header notes)
            try:
                df = pd.read_csv(filepath, nrows=100, skiprows=3)
                return df
            except Exception:
                pass
            
            # Strategy 3: Use Python engine with flexible parsing
            try:
                df = pd.read_csv(filepath, nrows=100, engine='python', sep=None)
                return df
            except Exception as e:
                print(f"      ⚠️  CSV parsing failed: {e}")
                return None
                
        elif ext in ['.xlsx', '.xls']:
            # Try reading first sheet
            df = pd.read_excel(filepath, nrows=100, engine='openpyxl' if ext == '.xlsx' else 'xlrd')
            return df
        else:
            print(f"      ⚠️  Unsupported format: {ext}")
            return None
    except Exception as e:
        print(f"      ❌ Error reading file: {e}")
        return None


# ================================================================
# 🔍 MAIN INSPECTION LOGIC
# ================================================================

def inspect_migration_data():
    """
    Inspect all migration datasets and generate summary.
    
    Returns:
        pd.DataFrame: Summary dataframe
    """
    print("\n" + "="*80)
    print("🔍 MIGRATION SECTOR DATA INSPECTION")
    print("="*80)
    
    # Get valid data files (filter out metadata)
    valid_files, metadata_files = get_migration_data_files(RAW_MIGRATION_PATH)
    
    # Print filtering summary
    print(f"\n📁 Scanning: {RAW_MIGRATION_PATH}")
    print(f"   ✅ Loaded {len(valid_files)} valid migration datasets (API_*.csv)")
    print(f"   ⏭️  Ignored {len(metadata_files)} metadata files (Metadata_*)")
    
    if metadata_files:
        print(f"\n   Skipped metadata files:")
        for meta_file in metadata_files[:5]:  # Show first 5
            print(f"      - {meta_file}")
        if len(metadata_files) > 5:
            print(f"      ... and {len(metadata_files) - 5} more")
    
    if not valid_files:
        print(f"\n❌ No valid API CSV files found in {RAW_MIGRATION_PATH}")
        print(f"   Expected format: API_*.csv")
        return pd.DataFrame()
    
    print(f"\n🔎 Inspecting {len(valid_files)} data files...\n")
    
    # Inspect each file
    summaries = []
    
    for i, filepath in enumerate(valid_files, 1):
        filename = os.path.basename(filepath)
        ext = os.path.splitext(filename)[1].lower()
        
        print(f"[{i}/{len(valid_files)}] Processing: {filename}")
        
        # Read file
        df = read_file(filepath)
        
        if df is None or df.empty:
            print(f"      ⚠️  Skipping (empty or unreadable)")
            continue
        
        # Extract metadata
        row_count = len(df)
        columns = list(df.columns)
        num_columns = len(columns)
        
        # Detect time column
        time_col = detect_time_column(columns)
        
        # Extract year range (handles both long and wide formats)
        min_year, max_year = extract_year_range(df, time_col if time_col else None)
        
        # Create summary record
        summary = {
            'file_name': filename,
            'file_type': ext.replace('.', ''),
            'row_count': row_count,
            'num_columns': num_columns,
            'columns': ', '.join(columns[:10]) + ('...' if num_columns > 10 else ''),
            'time_column': time_col if time_col else 'NOT DETECTED',
            'min_year': min_year if min_year else 'N/A',
            'max_year': max_year if max_year else 'N/A'
        }
        
        summaries.append(summary)
        
        # Print summary
        print(f"      ✅ Rows: {row_count:,}")
        print(f"      ✅ Columns: {num_columns}")
        print(f"      ✅ Time Column: {time_col if time_col else 'NOT DETECTED'}")
        if min_year and max_year:
            print(f"      ✅ Year Range: {min_year} - {max_year}")
        print()
    
    # Create summary DataFrame
    if summaries:
        df_summary = pd.DataFrame(summaries)
        
        # Save to CSV
        df_summary.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        file_size = os.path.getsize(OUTPUT_FILE) / 1024
        
        print("="*80)
        print("✅ INSPECTION COMPLETE")
        print("="*80)
        print(f"\n📊 Summary Statistics:")
        print(f"   Total files inspected: {len(summaries)}")
        print(f"   Files with time column: {sum(1 for s in summaries if s['time_column'] != 'NOT DETECTED')}")
        print(f"   Files with year range: {sum(1 for s in summaries if s['min_year'] != 'N/A')}")
        
        print(f"\n💾 Output saved to:")
        print(f"   {OUTPUT_FILE}")
        print(f"   File size: {file_size:.2f} KB")
        
        print(f"\n📄 Preview:")
        print("-"*80)
        print(df_summary[['file_name', 'row_count', 'time_column', 'min_year', 'max_year']].to_string(index=False))
        print("-"*80)
        
        return df_summary
    else:
        print("\n❌ No valid data found in any files")
        return pd.DataFrame()


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    try:
        df_summary = inspect_migration_data()
        
        if not df_summary.empty:
            print("\n" + "="*80)
            print("🎯 NEXT STEPS")
            print("="*80)
            print("1. Review migration_summary.csv for data quality")
            print("2. Identify primary dataset(s) for analysis")
            print("3. Plan data cleaning strategy")
            print("="*80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
