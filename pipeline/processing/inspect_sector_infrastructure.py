"""
🔍 Infrastructure Sector Data Inspection Script

Scans all files in data/raw/infrastructure/ and generates a comprehensive summary.

Input:  data/raw/infrastructure/* (CSV, XLSX files)
Output: data/processed/infrastructure/infrastructure_summary.csv

Purpose:
    - Understand available infrastructure datasets
    - Identify time columns and year ranges
    - Assess data quality (missing values)
    - Guide data selection for cleaning phase
"""

import os
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 2 levels from pipeline/processing/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

DATA_PATH = os.path.join(BASE_PATH, "data", "raw", "infrastructure")
OUTPUT_FOLDER = os.path.join(BASE_PATH, "data", "processed", "infrastructure")
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "infrastructure_summary.csv")

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

print("="*70)
print("🔍 INFRASTRUCTURE SECTOR DATA INSPECTION")
print("="*70)
print(f"\n📂 Scanning folder: {DATA_PATH}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def detect_time_column(df):
    """
    Detect potential time-related columns in DataFrame.
    
    Checks for common time column names:
    - year, Year, YEAR
    - month, Month, MONTH
    - date, Date, DATE
    - time, Time, TIME
    - period, Period
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        str or None: Name of detected time column, or None if not found
    """
    # Common time column patterns
    time_patterns = ['year', 'month', 'date', 'time', 'period']
    
    for col in df.columns:
        col_lower = col.lower().strip()
        for pattern in time_patterns:
            if pattern in col_lower:
                return col
    
    return None


def extract_year_range(df, time_col):
    """
    Extract min and max year from a time column.
    
    Handles:
    - Numeric years (e.g., 2020, 2021)
    - 2-digit years (e.g., 19 → 2019, 20 → 2020)
    - String years (e.g., "2020", "Calendar Year 2020")
    - Datetime objects
    
    Args:
        df (pd.DataFrame): Input DataFrame
        time_col (str): Name of time column
        
    Returns:
        tuple: (min_year, max_year) or (None, None) if extraction fails
    """
    try:
        series = df[time_col].copy()
        
        # Strategy 1: Try direct numeric conversion
        numeric_series = pd.to_numeric(series, errors='coerce')
        
        # Fix 2-digit years: convert 19 → 2019, 20 → 2020, etc.
        def fix_two_digit_year(year):
            if pd.isna(year):
                return year
            year = int(year)
            if year < 100:
                # Assume 2-digit years are in 2000s
                return 2000 + year
            return year
        
        numeric_series = numeric_series.apply(fix_two_digit_year)
        
        # Filter valid years (reasonable range)
        valid_years = numeric_series[(numeric_series >= 1900) & (numeric_series <= 2100)].dropna()
        
        if len(valid_years) > 0:
            min_year = int(valid_years.min())
            max_year = int(valid_years.max())
            return min_year, max_year
        
        # Strategy 2: Try parsing as datetime
        try:
            dt_series = pd.to_datetime(series, errors='coerce')
            valid_dates = dt_series.dropna()
            
            if len(valid_dates) > 0:
                min_year = int(valid_dates.dt.year.min())
                max_year = int(valid_dates.dt.year.max())
                return min_year, max_year
        except:
            pass
        
        # Strategy 3: Extract year from string using regex
        import re
        years_found = []
        for val in series.dropna():
            # Look for 4-digit numbers that could be years (20XX or 19XX)
            matches = re.findall(r'\b((?:19|20)\d{2})\b', str(val))
            if matches:
                years_found.extend([int(m) for m in matches])
        
        if years_found:
            return min(years_found), max(years_found)
        
        return None, None
    
    except Exception as e:
        print(f"      ⚠️  Warning: Could not extract year range: {e}")
        return None, None


def calculate_missing_percentage(df):
    """
    Calculate overall missing value percentage.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        float: Percentage of missing values (0-100)
    """
    total_cells = df.size
    if total_cells == 0:
        return 0.0
    
    missing_cells = df.isna().sum().sum()
    missing_pct = (missing_cells / total_cells) * 100
    
    return round(missing_pct, 2)


def load_file(filepath):
    """
    Load a file (CSV or XLSX) with error handling.
    
    Args:
        filepath (str): Path to file
        
    Returns:
        pd.DataFrame or None: Loaded DataFrame, or None if loading fails
    """
    try:
        ext = os.path.splitext(filepath)[1].lower()
        
        if ext == '.csv':
            df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath, engine='openpyxl' if ext == '.xlsx' else 'xlrd')
        else:
            print(f"   ⏭️  Skipped unsupported format: {ext}")
            return None
        
        return df
    
    except Exception as e:
        print(f"   ❌ ERROR loading {os.path.basename(filepath)}: {e}")
        return None


# ================================================================
# 🎯 MAIN INSPECTION LOGIC
# ================================================================

def inspect_infrastructure_data():
    """
    Main function to inspect all infrastructure files.
    """
    
    # Check if data folder exists
    if not os.path.exists(DATA_PATH):
        print(f"\n❌ ERROR: Data folder not found: {DATA_PATH}")
        return
    
    # Get all files in folder
    all_files = [f for f in os.listdir(DATA_PATH) 
                 if os.path.isfile(os.path.join(DATA_PATH, f)) 
                 and f.endswith(('.csv', '.xlsx', '.xls'))]
    
    if not all_files:
        print(f"\n⚠️  WARNING: No CSV/XLSX files found in {DATA_PATH}")
        return
    
    print(f"\n📊 Found {len(all_files)} file(s) to inspect\n")
    print("="*70)
    
    # Store results
    results = []
    files_with_time = 0
    files_without_time = 0
    errors_count = 0
    
    # Process each file
    for i, filename in enumerate(sorted(all_files), 1):
        filepath = os.path.join(DATA_PATH, filename)
        file_size = os.path.getsize(filepath) / 1024  # KB
        
        print(f"\n[{i}/{len(all_files)}] Inspecting: {filename} ({file_size:.1f} KB)")
        print("-"*70)
        
        # Load file
        df = load_file(filepath)
        
        if df is None:
            errors_count += 1
            continue
        
        print(f"   ✅ Loaded: {len(df):,} rows, {len(df.columns)} columns")
        
        # Get column names
        column_names = list(df.columns)
        columns_str = ", ".join(column_names[:10])  # Show first 10
        if len(column_names) > 10:
            columns_str += f" ... (+{len(column_names) - 10} more)"
        
        print(f"   📋 Columns: {columns_str}")
        
        # Detect time column
        time_col = detect_time_column(df)
        
        if time_col:
            print(f"   ⏰ Detected time column: '{time_col}'")
            files_with_time += 1
            
            # Extract year range
            min_year, max_year = extract_year_range(df, time_col)
            
            if min_year and max_year:
                print(f"   📅 Year range: {min_year} - {max_year}")
            else:
                print(f"   ⚠️  Could not extract year range")
                min_year, max_year = None, None
        else:
            print(f"   ⚠️  No time column detected")
            files_without_time += 1
            min_year, max_year = None, None
        
        # Calculate missing percentage
        missing_pct = calculate_missing_percentage(df)
        print(f"   📊 Missing values: {missing_pct}%")
        
        # Store result
        result = {
            'file_name': filename,
            'rows': len(df),
            'columns_count': len(df.columns),
            'column_names': ", ".join(column_names),
            'time_column': time_col if time_col else "None",
            'min_year': min_year,
            'max_year': max_year,
            'missing_percent': missing_pct
        }
        
        results.append(result)
    
    # ================================================================
    # 💾 SAVE SUMMARY
    # ================================================================
    
    print("\n" + "="*70)
    print("💾 Saving inspection summary...")
    
    if results:
        summary_df = pd.DataFrame(results)
        summary_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        file_size = os.path.getsize(OUTPUT_FILE) / 1024
        print(f"   ✅ Summary saved to: {OUTPUT_FILE}")
        print(f"   📦 File size: {file_size:.2f} KB")
        
        # ================================================================
        # 📊 PRINT FINAL SUMMARY
        # ================================================================
        
        print("\n" + "="*70)
        print("📊 INSPECTION SUMMARY")
        print("="*70)
        print(f"   Total files scanned: {len(all_files)}")
        print(f"   Successfully processed: {len(results)}")
        print(f"   Errors/skipped: {errors_count}")
        print(f"   Files WITH time column: {files_with_time}")
        print(f"   Files WITHOUT time column: {files_without_time}")
        
        if results:
            total_rows = sum(r['rows'] for r in results)
            avg_missing = sum(r['missing_percent'] for r in results) / len(results)
            
            print(f"\n   📈 AGGREGATE STATISTICS:")
            print(f"      Total rows across all files: {total_rows:,}")
            print(f"      Average missing values: {avg_missing:.2f}%")
            
            # Show year range if available
            years_with_data = [r for r in results if r['min_year'] is not None]
            if years_with_data:
                global_min = min(r['min_year'] for r in years_with_data)
                global_max = max(r['max_year'] for r in years_with_data)
                print(f"      Global year range: {global_min} - {global_max}")
        
        print("\n" + "="*70)
        print("✅ Infrastructure data inspection completed")
        print("="*70)
        
        # Print detailed table
        print("\n📋 DETAILED FILE SUMMARY:")
        print("-"*140)
        print(summary_df[['file_name', 'rows', 'columns_count', 'time_column', 'min_year', 'max_year', 'missing_percent']].to_string(index=False))
        print("-"*140)
    
    else:
        print("   ⚠️  No files were successfully processed")


# ================================================================
# 🚀 EXECUTE
# ================================================================

if __name__ == "__main__":
    try:
        inspect_infrastructure_data()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
