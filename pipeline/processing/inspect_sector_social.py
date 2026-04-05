"""
🔍 Social Sector Data Inspection Script

Loops through ALL CSV files in data/raw/social/ and extracts:
- File name
- Column names
- Time columns (containing "year" or "date")
- Minimum year
- Maximum year

Output: data/processed/social/social_summary.csv
"""

import pandas as pd
import os
import glob
from datetime import datetime

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 2 levels from pipeline/processing/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

RAW_FOLDER = os.path.join(BASE_PATH, "data", "raw", "social")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "social", "social_summary.csv")

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

print("="*70)
print("🔍 SOCIAL SECTOR DATA INSPECTION")
print("="*70)
print(f"\n📂 Scanning folder: {RAW_FOLDER}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def find_time_columns(df):
    """
    Find columns that might contain time/date information.
    
    Looks for columns containing 'year' or 'date' (case-insensitive).
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        list: List of potential time column names
    """
    time_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if 'year' in col_lower or 'date' in col_lower:
            time_cols.append(col)
    return time_cols


def extract_year_from_column(df, col_name):
    """
    Extract year values from a column, handling different formats.
    
    Tries multiple strategies:
    1. Direct numeric conversion (if already years like 2020, 2021)
    2. Parse as datetime and extract .dt.year
    3. Extract year from string patterns
    
    Args:
        df (pd.DataFrame): Input DataFrame
        col_name (str): Column name to extract year from
        
    Returns:
        tuple: (min_year, max_year) or (None, None) if extraction fails
    """
    try:
        # Strategy 1: Try direct numeric conversion
        series = pd.to_numeric(df[col_name], errors='coerce')
        
        # Check if values look like years (between 1900 and 2100)
        valid_years = series[(series >= 1900) & (series <= 2100)].dropna()
        
        if len(valid_years) > 0:
            min_year = int(valid_years.min())
            max_year = int(valid_years.max())
            return min_year, max_year
        
        # Strategy 2: Try parsing as datetime
        try:
            dt_series = pd.to_datetime(df[col_name], errors='coerce')
            valid_dates = dt_series.dropna()
            
            if len(valid_dates) > 0:
                min_year = int(valid_dates.dt.year.min())
                max_year = int(valid_dates.dt.year.max())
                return min_year, max_year
        except:
            pass
        
        # Strategy 3: Extract year from string patterns (e.g., "2020-01-01")
        try:
            str_series = df[col_name].astype(str)
            # Look for 4-digit year patterns
            year_pattern = str_series.str.extract(r'(\b\d{4}\b)', expand=False)
            years = pd.to_numeric(year_pattern, errors='coerce')
            valid_years = years[(years >= 1900) & (years <= 2100)].dropna()
            
            if len(valid_years) > 0:
                min_year = int(valid_years.min())
                max_year = int(valid_years.max())
                return min_year, max_year
        except:
            pass
        
        return None, None
        
    except Exception as e:
        print(f"      ⚠️  Warning: Could not extract year from '{col_name}': {str(e)}")
        return None, None


def inspect_single_file(filepath):
    """
    Inspect a single CSV file and extract metadata.
    
    Args:
        filepath (str): Path to CSV file
        
    Returns:
        dict: Dictionary with file metadata or None if file cannot be read
    """
    filename = os.path.basename(filepath)
    
    try:
        # Read CSV file
        print(f"\n📄 Processing: {filename}")
        df = pd.read_csv(filepath)
        
        # Get basic info
        num_rows = len(df)
        num_cols = len(df.columns)
        columns_list = list(df.columns)
        
        print(f"   Rows: {num_rows:,}, Columns: {num_cols}")
        print(f"   Columns: {columns_list[:5]}{'...' if num_cols > 5 else ''}")
        
        # Find time columns
        time_cols = find_time_columns(df)
        
        if time_cols:
            print(f"   ⏰ Time columns found: {time_cols}")
            
            # Try to extract year range from each time column
            best_min_year = None
            best_max_year = None
            best_time_col = None
            
            for time_col in time_cols:
                min_year, max_year = extract_year_from_column(df, time_col)
                
                if min_year is not None and max_year is not None:
                    print(f"      ✓ {time_col}: {min_year} - {max_year}")
                    
                    # Keep the first valid time column found
                    if best_min_year is None:
                        best_min_year = min_year
                        best_max_year = max_year
                        best_time_col = time_col
                else:
                    print(f"      ✗ {time_col}: No valid year data")
            
            return {
                'file': filename,
                'columns': '; '.join(columns_list),
                'time_column': best_time_col if best_time_col else '',
                'min_year': best_min_year if best_min_year else '',
                'max_year': best_max_year if best_max_year else ''
            }
        else:
            print(f"   ⚠️  No time columns detected")
            return {
                'file': filename,
                'columns': '; '.join(columns_list),
                'time_column': '',
                'min_year': '',
                'max_year': ''
            }
            
    except Exception as e:
        print(f"   ❌ ERROR reading file: {str(e)}")
        return {
            'file': filename,
            'columns': 'ERROR',
            'time_column': '',
            'min_year': '',
            'max_year': ''
        }


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    # Check if raw folder exists
    if not os.path.exists(RAW_FOLDER):
        print(f"\n❌ ERROR: Raw folder not found: {RAW_FOLDER}")
        return
    
    # Get all CSV files
    csv_files = glob.glob(os.path.join(RAW_FOLDER, "*.csv"))
    
    if not csv_files:
        print(f"\n⚠️  WARNING: No CSV files found in {RAW_FOLDER}")
        return
    
    print(f"\n📊 Found {len(csv_files)} CSV file(s)")
    print("="*70)
    
    # Process each file
    results = []
    success_count = 0
    error_count = 0
    
    for i, filepath in enumerate(sorted(csv_files), 1):
        print(f"\n[{i}/{len(csv_files)}]", end=" ")
        
        result = inspect_single_file(filepath)
        
        if result:
            results.append(result)
            if result['columns'] != 'ERROR':
                success_count += 1
            else:
                error_count += 1
    
    # ================================================================
    # 💾 SAVE RESULTS
    # ================================================================
    
    print("\n" + "="*70)
    print("💾 Saving results...")
    
    if results:
        # Create DataFrame
        df_results = pd.DataFrame(results)
        
        # Save to CSV
        df_results.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        file_size = os.path.getsize(OUTPUT_FILE) / 1024
        print(f"   ✅ Output saved to: {OUTPUT_FILE}")
        print(f"   📦 File size: {file_size:.2f} KB")
        print(f"   📊 Files processed: {len(results)}")
        print(f"   ✅ Successful: {success_count}")
        print(f"   ❌ Errors: {error_count}")
        
        # Display summary
        print("\n" + "="*70)
        print("📋 INSPECTION SUMMARY")
        print("="*70)
        
        # Show files with time data
        files_with_time = df_results[df_results['time_column'] != '']
        
        if len(files_with_time) > 0:
            print(f"\n✅ Files WITH time data ({len(files_with_time)}):")
            for _, row in files_with_time.iterrows():
                print(f"   • {row['file']}: {row['time_column']} ({row['min_year']}-{row['max_year']})")
        
        # Show files without time data
        files_without_time = df_results[df_results['time_column'] == '']
        if len(files_without_time) > 0:
            print(f"\n⚠️  Files WITHOUT time data ({len(files_without_time)}):")
            for _, row in files_without_time.iterrows():
                print(f"   • {row['file']}")
        
        # Overall year range
        if len(files_with_time) > 0:
            overall_min = files_with_time['min_year'].min()
            overall_max = files_with_time['max_year'].max()
            print(f"\n📅 Overall year range across all files: {overall_min} - {overall_max}")
        
        print("\n" + "="*70)
        print("✅ Social inspection completed")
        print("="*70)
        
    else:
        print("   ⚠️  No results to save")
        print("\n✅ Social inspection completed")


if __name__ == "__main__":
    main()
