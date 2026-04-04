"""
🔍 Economy Sector Data Inspector

This script scans all economy datasets in data/raw/economy/ and generates
a comprehensive summary file with metadata about each dataset.

Supported formats: .csv, .xlsx
Output: data/processed/economy/economy_summary.csv
"""

import pandas as pd
import os
import re
from pathlib import Path

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
RAW_ECONOMY_PATH = os.path.join(BASE_PATH, "data", "raw", "economy")
PROCESSED_ECONOMY_PATH = os.path.join(BASE_PATH, "data", "processed", "economy")
OUTPUT_FILE = os.path.join(PROCESSED_ECONOMY_PATH, "economy_summary.csv")

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def detect_time_column(columns):
    """
    Detect time-related column from list of columns.
    
    Args:
        columns (list): List of column names
        
    Returns:
        str or None: Detected time column name, or None if not found
    """
    # Patterns to search for (case-insensitive)
    time_patterns = ['year', 'date', 'time', 'period', 'month', 'day']
    
    for col in columns:
        col_lower = str(col).lower()
        for pattern in time_patterns:
            if pattern in col_lower:
                return col
    
    return None


def extract_year_from_value(value):
    """
    Extract year from a value that might be a date/year.
    
    Args:
        value: Value that might contain year information
        
    Returns:
        int or None: Extracted year, or None if not found
    """
    if pd.isna(value):
        return None
    
    value_str = str(value)
    
    # Try to find 4-digit year (1900-2099)
    year_match = re.search(r'(19|20)\d{2}', value_str)
    if year_match:
        return int(year_match.group())
    
    # Try to parse as datetime
    try:
        if '/' in value_str or '-' in value_str:
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y', '%Y/%m/%d', '%d/%m/%Y']:
                try:
                    dt = pd.to_datetime(value_str, format=fmt)
                    return dt.year
                except:
                    continue
    except:
        pass
    
    return None


def get_min_max_years(df, time_column):
    """
    Extract min and max years from detected time column.
    
    Args:
        df (pd.DataFrame): DataFrame to analyze
        time_column (str): Name of time column
        
    Returns:
        tuple: (min_year, max_year) or (None, None) if not determinable
    """
    if time_column is None or time_column not in df.columns:
        return None, None
    
    years = []
    
    # Try to extract years from each value in the time column
    for value in df[time_column].dropna().head(100):  # Sample first 100 rows
        year = extract_year_from_value(value)
        if year:
            years.append(year)
    
    if len(years) > 0:
        return min(years), max(years)
    
    return None, None


def inspect_file(file_path):
    """
    Inspect a single file and extract metadata.
    
    Args:
        file_path (str): Path to the file
        
    Returns:
        dict: File metadata or None if inspection failed
    """
    try:
        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        
        print(f"   Inspecting: {file_name}")
        
        # Read file based on extension
        if file_ext == '.csv':
            try:
                df = pd.read_csv(file_path, encoding='utf-8', nrows=5)  # Read first 5 rows
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin-1', nrows=5)
        elif file_ext == '.xlsx':
            df = pd.read_excel(file_path, nrows=5)
        else:
            print(f"      ⚠️  Unsupported format: {file_ext}")
            return None
        
        # Get basic metadata
        num_rows = len(pd.read_csv(file_path, encoding='utf-8') if file_ext == '.csv' 
                      else pd.read_excel(file_path))
        columns = list(df.columns)
        
        # Detect time column
        time_column = detect_time_column(columns)
        
        # Get min/max years
        min_year, max_year = get_min_max_years(df, time_column)
        
        # Format columns as comma-separated string
        columns_str = ', '.join([str(col) for col in columns])
        
        result = {
            'file': file_name,
            'columns': columns_str,
            'time_column': time_column if time_column else 'Not detected',
            'min_year': min_year if min_year else 'N/A',
            'max_year': max_year if max_year else 'N/A',
            'rows': num_rows
        }
        
        print(f"      ✅ Columns: {len(columns)}, Rows: {num_rows}, Time: {time_column}")
        
        return result
        
    except Exception as e:
        print(f"      ❌ Error reading file: {str(e)}")
        return None


def scan_directory(directory_path):
    """
    Recursively scan directory for supported files.
    
    Args:
        directory_path (str): Path to directory to scan
        
    Returns:
        list: List of file paths found
    """
    supported_extensions = ['.csv', '.xlsx']
    files_found = []
    
    print(f"\n📁 Scanning directory: {directory_path}")
    print("-" * 60)
    
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in supported_extensions:
                file_path = os.path.join(root, file)
                files_found.append(file_path)
    
    print(f"   Found {len(files_found)} supported files")
    
    return files_found


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    print("\n" + "="*70)
    print("🔍 ECONOMY SECTOR DATA INSPECTION")
    print("="*70)
    
    # Create processed directory if it doesn't exist
    os.makedirs(PROCESSED_ECONOMY_PATH, exist_ok=True)
    print(f"\n✅ Output directory ready: {PROCESSED_ECONOMY_PATH}")
    
    # Check if raw directory exists
    if not os.path.exists(RAW_ECONOMY_PATH):
        print(f"\n❌ ERROR: Raw economy directory not found: {RAW_ECONOMY_PATH}")
        return
    
    # Scan for files
    files = scan_directory(RAW_ECONOMY_PATH)
    
    if len(files) == 0:
        print("\n⚠️  No supported files found (.csv, .xlsx)")
        return
    
    # Inspect each file
    print("\n🔍 Inspecting files...")
    print("-" * 60)
    
    inspection_results = []
    success_count = 0
    error_count = 0
    
    for file_path in files:
        result = inspect_file(file_path)
        if result:
            inspection_results.append(result)
            success_count += 1
        else:
            error_count += 1
    
    # Create summary DataFrame
    print("\n📊 Creating summary table...")
    summary_df = pd.DataFrame(inspection_results)
    
    # Reorder columns
    column_order = ['file', 'columns', 'time_column', 'min_year', 'max_year', 'rows']
    summary_df = summary_df[column_order]
    
    # Sort by filename
    summary_df = summary_df.sort_values('file')
    
    # Save to CSV
    print(f"\n💾 Saving summary to: {OUTPUT_FILE}")
    summary_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    # Print summary statistics
    print("\n" + "="*70)
    print("📊 INSPECTION SUMMARY")
    print("="*70)
    print(f"\n   Total files scanned: {len(files)}")
    print(f"   Successfully inspected: {success_count}")
    print(f"   Errors encountered: {error_count}")
    print(f"\n   Files with time columns detected: {summary_df['time_column'].apply(lambda x: x != 'Not detected').sum()}")
    
    # Year range statistics
    valid_years = summary_df[summary_df['min_year'] != 'N/A']
    if len(valid_years) > 0:
        all_min_years = [int(y) for y in valid_years['min_year']]
        all_max_years = [int(y) for y in valid_years['max_year']]
        print(f"\n   Overall year range: {min(all_min_years)} - {max(all_max_years)}")
    
    print(f"\n✅ Summary saved → {OUTPUT_FILE}")
    print("\nEconomy inspection completed")
    print("="*70)


if __name__ == "__main__":
    main()
