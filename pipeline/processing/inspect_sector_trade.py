"""
📊 Trade Dataset Inspector (Enhanced Version)

Scans all CSV and Excel files in data/raw/trade/ directory
and generates a comprehensive summary with robust year detection.

Output: data/processed/trade/trade_summary.csv
"""

import pandas as pd
import os
import glob
import re
from pathlib import Path

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

# Column name mappings (lowercase matching)
COLUMN_MAPPINGS = {
    'year': ['year', 'yr', 'time', 'date'],
    'country': ['country', 'reporter', 'nation'],
    'partner': ['partner', 'export country', 'import country', 'destination', 'origin'],
    'trade_value': ['value', 'trade value', 'export value', 'import value'],
    'trade_type': ['type', 'flow', 'export', 'import']
}

# Input/Output paths
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
TRADE_RAW_PATH = os.path.join(BASE_PATH, "data", "raw", "trade")
TRADE_PROCESSED_PATH = os.path.join(BASE_PATH, "data", "processed", "trade")


# ================================================================
# 🔍 COLUMN DETECTION FUNCTIONS
# ================================================================

def detect_column_type(column_name, column_type):
    """
    Detect if a column matches a specific type using flexible substring matching.
    
    Args:
        column_name (str): Column name to check
        column_type (str): Type to detect ('year', 'country', 'partner', etc.)
    
    Returns:
        bool: True if column matches the type
    """
    column_lower = column_name.lower().strip()
    keywords = COLUMN_MAPPINGS.get(column_type, [])
    
    # Check if any keyword is in the column name
    return any(keyword in column_lower for keyword in keywords)


def find_all_columns_by_type(df, column_type):
    """
    Find ALL columns in DataFrame that match the given type.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column_type (str): Type to search for
    
    Returns:
        list: List of matching column names
    """
    matches = []
    for col in df.columns:
        if detect_column_type(col, column_type):
            matches.append(col)
    return matches


def find_best_column_by_type(df, column_type):
    """
    Find the best matching column for a given type.
    If multiple matches, prefer exact matches or first column.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column_type (str): Type to search for
    
    Returns:
        str or None: Best matching column name or None
    """
    matches = find_all_columns_by_type(df, column_type)
    
    if not matches:
        return None
    
    if len(matches) == 1:
        return matches[0]
    
    # Prefer exact matches
    for match in matches:
        if match.lower().strip() in COLUMN_MAPPINGS.get(column_type, []):
            return match
    
    # Otherwise return first match
    return matches[0]


# ================================================================
# 📈 YEAR EXTRACTION FUNCTIONS (ENHANCED)
# ================================================================

def extract_years_from_column(df, year_column):
    """
    Extract valid years from a column using regex pattern matching.
    Handles messy data by extracting 4-digit years from strings.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        year_column (str): Name of year column
    
    Returns:
        tuple: (min_year, max_year, valid_count) or (None, None, 0)
    """
    try:
        # Make a copy to avoid modifying original
        col_data = df[year_column].copy()
        
        # Step 1: Convert to string
        col_data = col_data.astype(str)
        
        # Step 2: Extract 4-digit years using regex
        # This will capture years like "2020", "Year 2020", "2020-21", etc.
        col_data = col_data.str.extract(r'(\d{4})', expand=False)
        
        # Step 3: Convert to numeric (coerce invalid values to NaN)
        col_data = pd.to_numeric(col_data, errors='coerce')
        
        # Step 4: Filter to reasonable year range (1900-2100)
        valid_years = col_data.dropna()
        valid_years = valid_years[(valid_years >= 1900) & (valid_years <= 2100)]
        
        if len(valid_years) == 0:
            return None, None, 0
        
        # Step 5: Get min and max
        min_year = int(valid_years.min())
        max_year = int(valid_years.max())
        
        return min_year, max_year, len(valid_years)
    
    except Exception as e:
        print(f"   ⚠️ Error extracting years: {e}")
        return None, None, 0


def find_best_year_column(df):
    """
    Find the best year column in the DataFrame.
    If multiple candidates, choose the one with most valid years.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        tuple: (best_column_name, min_year, max_year) or (None, None, None)
    """
    # Find all potential year columns
    year_candidates = find_all_columns_by_type(df, 'year')
    
    if not year_candidates:
        return None, None, None
    
    print(f"   🔍 Found {len(year_candidates)} potential year column(s): {year_candidates}")
    
    # Test each candidate
    best_col = None
    best_min_year = None
    best_max_year = None
    best_count = 0
    
    for col in year_candidates:
        min_y, max_y, count = extract_years_from_column(df, col)
        
        if count > best_count:
            best_col = col
            best_min_year = min_y
            best_max_year = max_y
            best_count = count
            
            print(f"   📊 Testing '{col}': {count} valid years ({min_y}-{max_y})")
    
    if best_count > 0:
        print(f"   ✅ Selected '{best_col}' with {best_count} valid years")
        return best_col, best_min_year, best_max_year
    else:
        return None, None, None


# ================================================================
# 📂 DATA LOADING FUNCTIONS
# ================================================================

def load_file_safely(file_path):
    """
    Load a file with proper error handling for encoding issues.
    
    Args:
        file_path (str): Path to the file
    
    Returns:
        pd.DataFrame or None: Loaded DataFrame or None if failed
    """
    file_name = os.path.basename(file_path)
    
    try:
        if file_path.endswith('.csv'):
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, nrows=500)  # Sample first 500 rows
                    print(f"   ✓ Loaded with {encoding} encoding")
                    return df
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"   ⚠️ Error with {encoding}: {e}")
                    continue
            
            print(f"   ❌ Could not read {file_name} with any encoding")
            return None
                
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, nrows=500)  # Sample first 500 rows
            print(f"   ✓ Loaded Excel file")
            return df
        
        else:
            print(f"   ⚠️ Unsupported file format: {file_name}")
            return None
    
    except Exception as e:
        print(f"   ❌ Error loading {file_name}: {e}")
        return None


# ================================================================
# 📊 ANALYSIS FUNCTIONS
# ================================================================

def analyze_file(file_path):
    """
    Analyze a single trade dataset file with enhanced year detection.
    
    Args:
        file_path (str): Path to the file
    
    Returns:
        dict: Summary information or None if failed
    """
    file_name = os.path.basename(file_path)
    print(f"\n📄 Processing: {file_name}")
    
    # Load data safely
    df = load_file_safely(file_path)
    
    if df is None:
        return None
    
    # Basic statistics
    num_rows = len(df)
    num_cols = len(df.columns)
    column_names = ', '.join([str(col) for col in df.columns.tolist()])
    
    # Detect special columns
    year_col, min_year, max_year = find_best_year_column(df)
    country_col = find_best_column_by_type(df, 'country')
    partner_col = find_best_column_by_type(df, 'partner')
    value_col = find_best_column_by_type(df, 'trade_value')
    type_col = find_best_column_by_type(df, 'trade_type')
    
    # Print detection results
    if year_col:
        print(f"   ✅ Year detected: {year_col} → Range: {min_year}-{max_year}")
    else:
        print(f"   ⚠️ No valid year column found")
    
    if country_col:
        print(f"   🌍 Country column: {country_col}")
    
    if partner_col:
        print(f"   🤝 Partner column: {partner_col}")
    
    if value_col:
        print(f"   💰 Value column: {value_col}")
    
    # Build result dictionary
    result = {
        'file_name': file_name,
        'number_of_rows': num_rows,
        'number_of_columns': num_cols,
        'column_names': column_names,
        'detected_year_column': year_col if year_col else 'Not Found',
        'min_year': min_year if min_year else 'Not Found',
        'max_year': max_year if max_year else 'Not Found',
        'country_column': country_col if country_col else 'Not Found',
        'partner_column': partner_col if partner_col else 'Not Found',
        'trade_value_column': value_col if value_col else 'Not Found',
        'trade_type_column': type_col if type_col else 'Not Found'
    }
    
    print(f"   ✓ Rows: {num_rows}, Columns: {num_cols}")
    
    return result


# ================================================================
# 📂 MAIN PROCESSING FUNCTION
# ================================================================

def scan_trade_datasets():
    """
    Scan all trade datasets and generate summary.
    
    Returns:
        list: List of dictionaries containing file summaries
    """
    print("\n" + "="*70)
    print("🔍 TRADE DATASET INSPECTOR (Enhanced)")
    print("="*70)
    
    # Ensure output directory exists
    os.makedirs(TRADE_PROCESSED_PATH, exist_ok=True)
    
    # Get all CSV and Excel files
    csv_files = glob.glob(os.path.join(TRADE_RAW_PATH, "*.csv"))
    excel_files = glob.glob(os.path.join(TRADE_RAW_PATH, "*.xlsx"))
    
    all_files = sorted(csv_files + excel_files)
    
    print(f"\n📂 Scanning: {TRADE_RAW_PATH}")
    print(f"   - CSV files: {len(csv_files)}")
    print(f"   - Excel files: {len(excel_files)}")
    print(f"   - Total files: {len(all_files)}")
    
    if len(all_files) == 0:
        print("\n⚠️ No trade files found! Please add files to data/raw/trade/")
        return []
    
    # Process each file
    results = []
    successful = 0
    failed = 0
    
    for i, file_path in enumerate(all_files, 1):
        print(f"\n[{i}/{len(all_files)}]", end=" ")
        result = analyze_file(file_path)
        
        if result:
            results.append(result)
            successful += 1
        else:
            failed += 1
    
    print(f"\n\n📊 Processing Complete:")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    
    # Sort by file name
    results.sort(key=lambda x: x['file_name'])
    
    return results


# ================================================================
# 💾 OUTPUT FUNCTIONS
# ================================================================

def save_summary(results, output_path):
    """
    Save summary results to CSV.
    
    Args:
        results (list): List of dictionaries
        output_path (str): Output file path
    """
    if not results:
        print("\n⚠️ No results to save!")
        return
    
    print("\n" + "="*70)
    print("💾 SAVING SUMMARY")
    print("="*70)
    
    # Create DataFrame and save
    df_summary = pd.DataFrame(results)
    
    # Reorder columns for better readability
    column_order = [
        'file_name',
        'number_of_rows',
        'number_of_columns',
        'detected_year_column',
        'min_year',
        'max_year',
        'country_column',
        'partner_column',
        'trade_value_column',
        'trade_type_column',
        'column_names'
    ]
    
    df_summary = df_summary[column_order]
    df_summary.to_csv(output_path, index=False)
    
    print(f"\n✅ Summary saved to: {output_path}")
    print(f"   Total files analyzed: {len(results)}")
    print(f"   Output shape: {df_summary.shape}")
    
    # Display sample
    print("\n📊 PREVIEW (First 10 files):")
    print("-" * 70)
    preview_df = df_summary[['file_name', 'number_of_rows', 'number_of_columns', 
                              'detected_year_column', 'min_year', 'max_year']].head(10)
    print(preview_df.to_string(index=False))


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """
    Main execution function.
    """
    print("\n🚀 Starting Enhanced Trade Dataset Inspection...")
    print("="*70)
    
    # Scan and analyze datasets
    results = scan_trade_datasets()
    
    # Define output path
    output_path = os.path.join(TRADE_PROCESSED_PATH, "trade_summary.csv")
    
    # Save results
    save_summary(results, output_path)
    
    # Final summary
    if results:
        print("\n" + "="*70)
        print("🎉 INSPECTION COMPLETE")
        print("="*70)
        
        # Statistics
        total_rows = sum(r['number_of_rows'] for r in results)
        files_with_years = sum(1 for r in results if r['min_year'] != 'Not Found')
        
        print(f"\n📈 SUMMARY STATISTICS:")
        print(f"   ✓ Total datasets: {len(results)}")
        print(f"   ✓ Total rows (sampled): {total_rows:,}")
        print(f"   ✓ Datasets with year info: {files_with_years}/{len(results)} ({files_with_years/len(results)*100:.1f}%)")
        
        # Year range summary
        years_found = [r for r in results if r['min_year'] != 'Not Found']
        if years_found:
            all_min_years = [r['min_year'] for r in years_found]
            all_max_years = [r['max_year'] for r in years_found]
            print(f"   ✓ Overall year range: {min(all_min_years)} - {max(all_max_years)}")
        
        # Most common columns
        year_cols = [r['detected_year_column'] for r in results if r['detected_year_column'] != 'Not Found']
        if year_cols:
            most_common_year = max(set(year_cols), key=year_cols.count)
            print(f"   ✓ Most common year column: '{most_common_year}'")
        
        print("\n✅ All done! Check the output file for detailed information.")
        print(f"📁 Output: {output_path}")
    else:
        print("\n❌ No files were successfully processed.")
    
    print("="*70)


if __name__ == "__main__":
    main()