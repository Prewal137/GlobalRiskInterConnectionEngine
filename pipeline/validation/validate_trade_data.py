"""
✅ Trade Data Validation Script

Validates cleaned trade dataset to ensure it's ready for feature engineering.
Performs comprehensive checks and provides clear pass/fail decision.

Input: data/processed/trade/cleaned_trade.csv
Output: Validation report with pass/fail decision
"""

import pandas as pd
import os
import sys

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

# Input file path
BASE_PATH = os.path.abspath(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/cleaned_trade.csv")

# Expected columns
EXPECTED_COLUMNS = ['Country', 'Partner', 'Year', 'Trade_Value', 'Trade_Type']

# Validation thresholds
MIN_VALID_YEARS = 5  # At least 5 years of data
MAX_MISSING_PERCENT = 1.0  # Max 1% missing values allowed
MIN_TRADE_VALUE = 0.01  # Minimum reasonable trade value
OUTLIER_PERCENT_THRESHOLD = 5.0  # Max 5% outliers allowed


# ================================================================
# 📊 VALIDATION FUNCTIONS
# ================================================================

def check_basic_structure(df):
    """
    Check if DataFrame has correct columns and structure.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("📋 CHECK 1: BASIC STRUCTURE")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    # Check columns
    expected_cols = EXPECTED_COLUMNS
    actual_cols = df.columns.tolist()
    
    print(f"\nExpected columns: {expected_cols}")
    print(f"Actual columns:   {actual_cols}")
    
    if set(expected_cols) == set(actual_cols):
        print("✅ Column names match perfectly")
    else:
        missing = set(expected_cols) - set(actual_cols)
        extra = set(actual_cols) - set(expected_cols)
        
        if missing:
            results['passed'] = False
            results['issues'].append(f"Missing columns: {missing}")
            print(f"❌ Missing columns: {missing}")
        
        if extra:
            results['issues'].append(f"Extra columns: {extra}")
            print(f"⚠️ Extra columns: {extra}")
    
    # Check row count
    print(f"\nTotal rows: {len(df):,}")
    
    if len(df) == 0:
        results['passed'] = False
        results['issues'].append("Dataset is empty!")
        print("❌ Dataset is empty!")
    else:
        print("✅ Dataset has data")
    
    return results


def check_missing_values(df):
    """
    Check for missing/null values in all columns.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("🔍 CHECK 2: MISSING VALUES")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    # Count missing values per column
    missing_counts = df.isnull().sum()
    total_rows = len(df)
    
    print(f"\n{'Column':<20} {'Missing':<10} {'Percent':<10}")
    print("-" * 40)
    
    critical_missing = []
    
    for col in EXPECTED_COLUMNS:
        missing = missing_counts[col]
        percent = (missing / total_rows) * 100
        
        status = "✅" if missing == 0 else ("⚠️" if percent < MAX_MISSING_PERCENT else "❌")
        print(f"{col:<20} {missing:<10,} {percent:<10.2f}% {status}")
        
        if missing > 0:
            if percent >= MAX_MISSING_PERCENT:
                results['passed'] = False
                critical_missing.append(col)
                results['issues'].append(f"High missing in {col}: {percent:.2f}%")
            else:
                results['issues'].append(f"Minor missing in {col}: {percent:.2f}%")
    
    if not critical_missing:
        print("\n✅ Missing values within acceptable range")
    else:
        print(f"\n❌ Critical missing values in: {critical_missing}")
    
    return results


def validate_country_column(df):
    """
    Validate Country column for quality and consistency.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("🌍 CHECK 3: COUNTRY VALIDATION")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    countries = df['Country']
    
    # Count unique countries
    unique_countries = countries.nunique()
    print(f"\nUnique countries: {unique_countries:,}")
    
    if unique_countries < 10:
        results['issues'].append(f"Very few countries: {unique_countries}")
        print("⚠️ Very few unique countries")
    else:
        print("✅ Good number of unique countries")
    
    # Detect numeric countries
    numeric_mask = countries.astype(str).str.isnumeric()
    numeric_count = numeric_mask.sum()
    
    if numeric_count > 0:
        results['passed'] = False
        results['issues'].append(f"Numeric countries found: {numeric_count}")
        print(f"❌ Numeric countries detected: {numeric_count}")
        print(f"   Examples: {countries[numeric_mask].unique()[:5].tolist()}")
    else:
        print("✅ No numeric country values")
    
    # Detect empty strings
    empty_mask = (countries.astype(str).str.strip() == '')
    empty_count = empty_mask.sum()
    
    if empty_count > 0:
        results['passed'] = False
        results['issues'].append(f"Empty country values: {empty_count}")
        print(f"❌ Empty country values: {empty_count}")
    else:
        print("✅ No empty country values")
    
    # Check for NaN
    nan_count = countries.isna().sum()
    if nan_count > 0:
        results['passed'] = False
        results['issues'].append(f"NaN country values: {nan_count}")
        print(f"❌ NaN country values: {nan_count}")
    else:
        print("✅ No NaN country values")
    
    # Show top 10 countries
    print(f"\n📊 Top 10 Countries by Records:")
    top_countries = countries.value_counts().head(10)
    for country, count in top_countries.items():
        print(f"   {country}: {count:,}")
    
    return results


def validate_year_column(df):
    """
    Validate Year column for quality and temporal coverage.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("📅 CHECK 4: YEAR VALIDATION")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    years = df['Year']
    
    # Check if numeric
    non_numeric = ~pd.to_numeric(years, errors='coerce').notna()
    non_numeric_count = non_numeric.sum()
    
    if non_numeric_count > 0:
        results['passed'] = False
        results['issues'].append(f"Non-numeric years: {non_numeric_count}")
        print(f"❌ Non-numeric year values: {non_numeric_count}")
    else:
        print("✅ All years are numeric")
    
    # Get min/max year
    min_year = int(years.min())
    max_year = int(years.max())
    year_range = max_year - min_year + 1
    
    print(f"\nYear range: {min_year} - {max_year}")
    print(f"Years covered: {year_range}")
    
    if year_range < MIN_VALID_YEARS:
        results['issues'].append(f"Insufficient year range: {year_range}")
        print(f"⚠️ Limited temporal coverage: only {year_range} years")
    else:
        print(f"✅ Good temporal coverage: {year_range} years")
    
    # Check for unreasonable years
    invalid_years = years[(years < 1900) | (years > 2100)]
    if len(invalid_years) > 0:
        results['passed'] = False
        results['issues'].append(f"Invalid years outside 1900-2100: {len(invalid_years)}")
        print(f"❌ Invalid years detected: {len(invalid_years)}")
    else:
        print("✅ All years in valid range (1900-2100)")
    
    # Distribution by year
    print(f"\n📊 Records per Year (Sample):")
    year_dist = years.value_counts().sort_index().head(10)
    for year, count in year_dist.items():
        print(f"   {year}: {count:,}")
    
    return results


def validate_trade_value(df):
    """
    Validate Trade_Value column for quality and outliers.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("💰 CHECK 5: TRADE_VALUE VALIDATION")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    values = df['Trade_Value']
    
    # Check if numeric
    non_numeric = ~pd.to_numeric(values, errors='coerce').notna()
    non_numeric_count = non_numeric.sum()
    
    if non_numeric_count > 0:
        results['passed'] = False
        results['issues'].append(f"Non-numeric trade values: {non_numeric_count}")
        print(f"❌ Non-numeric values: {non_numeric_count}")
    else:
        print("✅ All trade values are numeric")
    
    # Check for zero or negative values
    non_positive = values <= 0
    non_positive_count = non_positive.sum()
    
    if non_positive_count > 0:
        results['passed'] = False
        results['issues'].append(f"Non-positive trade values: {non_positive_count}")
        print(f"❌ Values <= 0: {non_positive_count}")
    else:
        print("✅ All values are positive")
    
    # Check for very small values (potential outliers)
    very_small = values < MIN_TRADE_VALUE
    very_small_count = very_small.sum()
    very_small_percent = (very_small_count / len(df)) * 100
    
    if very_small_percent > OUTLIER_PERCENT_THRESHOLD:
        results['issues'].append(f"Many very small values (< {MIN_TRADE_VALUE}): {very_small_percent:.2f}%")
        print(f"⚠️ Many very small values (< {MIN_TRADE_VALUE}): {very_small_percent:.2f}%")
    elif very_small_count > 0:
        print(f"ℹ️ Few very small values (< {MIN_TRADE_VALUE}): {very_small_count}")
    else:
        print(f"✅ No extremely small values (< {MIN_TRADE_VALUE})")
    
    # Statistics
    print(f"\n📊 Trade Value Statistics:")
    print(f"   Min: ${values.min():,.2f}")
    print(f"   Max: ${values.max():,.2f}")
    print(f"   Mean: ${values.mean():,.2f}")
    print(f"   Median: ${values.median():,.2f}")
    print(f"   Total: ${values.sum():,.2f}")
    
    # Check for extreme outliers (very large values)
    q99 = values.quantile(0.99)
    extreme_outliers = values > (q99 * 10)
    if extreme_outliers.sum() > 0:
        print(f"⚠️ Extreme outliers detected (> ${q99*10:,.2f}): {extreme_outliers.sum()}")
    else:
        print(f"✅ No extreme outliers")
    
    return results


def validate_trade_type(df):
    """
    Validate Trade_Type column for valid categories.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("📊 CHECK 6: TRADE_TYPE VALIDATION")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    trade_types = df['Trade_Type']
    
    # Count unique values
    unique_types = trade_types.unique()
    print(f"\nUnique Trade_Type values: {unique_types.tolist()}")
    
    # Check if only Export/Import exist
    valid_types = {'Export', 'Import'}
    invalid_types = set(unique_types) - valid_types
    
    if invalid_types:
        results['passed'] = False
        results['issues'].append(f"Invalid trade types: {invalid_types}")
        print(f"❌ Invalid trade types found: {invalid_types}")
    else:
        print("✅ Only valid trade types (Export/Import)")
    
    # Distribution
    print(f"\n📊 Trade Type Distribution:")
    type_counts = trade_types.value_counts()
    
    for trade_type, count in type_counts.items():
        percent = (count / len(df)) * 100
        print(f"   {trade_type}: {count:,} ({percent:.1f}%)")
    
    # Check if both types exist
    if 'Export' not in unique_types and 'Import' not in unique_types:
        results['passed'] = False
        results['issues'].append("No Export or Import records!")
        print("❌ No Export or Import records!")
    elif 'Export' not in unique_types:
        results['issues'].append("No Export records")
        print("⚠️ No Export records")
    elif 'Import' not in unique_types:
        results['issues'].append("No Import records")
        print("⚠️ No Import records")
    else:
        print("✅ Both Export and Import present")
    
    return results


def validate_partner_column(df):
    """
    Validate Partner column for consistency.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("🤝 CHECK 7: PARTNER VALIDATION")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    partners = df['Partner']
    
    # Count unique partners
    unique_partners = partners.nunique()
    print(f"\nUnique partners: {unique_partners:,}")
    
    # Check if all are "Global"
    global_count = (partners == 'Global').sum()
    global_percent = (global_count / len(df)) * 100
    
    print(f"Records with 'Global' partner: {global_count:,} ({global_percent:.1f}%)")
    
    if global_percent > 90:
        results['issues'].append(f"Most partners are 'Global': {global_percent:.1f}%")
        print(f"⚠️ Most partners are 'Global' ({global_percent:.1f}%)")
        print(f"   This may limit analysis capabilities")
    elif global_percent > 50:
        print(f"ℹ️ Majority of partners are 'Global'")
    else:
        print(f"✅ Good variety of partners")
    
    # Check for missing partners
    missing_partners = partners.isna().sum() + (partners == '').sum()
    if missing_partners > 0:
        results['issues'].append(f"Missing partner values: {missing_partners}")
        print(f"❌ Missing partner values: {missing_partners}")
    else:
        print("✅ No missing partner values")
    
    # Show top partners
    if unique_partners > 1:
        print(f"\n📊 Top 10 Partners by Records:")
        top_partners = partners.value_counts().head(10)
        for partner, count in top_partners.items():
            print(f"   {partner}: {count:,}")
    
    return results


def check_duplicates(df):
    """
    Check for duplicate records.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("🔄 CHECK 8: DUPLICATE CHECK")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    # Check for duplicates
    duplicate_cols = ['Country', 'Partner', 'Year', 'Trade_Type']
    duplicates = df.duplicated(subset=duplicate_cols, keep=False)
    duplicate_count = duplicates.sum() // 2  # Each duplicate appears twice
    
    print(f"\nChecking duplicates based on: {duplicate_cols}")
    print(f"Duplicate record pairs found: {duplicate_count:,}")
    
    if duplicate_count > 0:
        results['issues'].append(f"Duplicate records: {duplicate_count * 2:,}")
        print(f"❌ Duplicate records detected: {duplicate_count * 2:,}")
        
        # Show examples
        dup_df = df[duplicates].drop_duplicates(subset=duplicate_cols, keep='first')
        if len(dup_df) > 0:
            print(f"\n   Example duplicates:")
            print(dup_df.head(3).to_string(index=False))
    else:
        print("✅ No duplicate records found")
    
    return results


def check_data_distribution(df):
    """
    Check overall data distribution and continuity.
    
    Args:
        df (pd.DataFrame): Input DataFrame
    
    Returns:
        dict: Validation results
    """
    print("\n" + "="*70)
    print("📈 CHECK 9: DATA DISTRIBUTION")
    print("="*70)
    
    results = {
        'passed': True,
        'issues': []
    }
    
    # Sample countries for year continuity check
    sample_countries = df['Country'].value_counts().head(5).index.tolist()
    
    print(f"\n📊 Year Continuity Check (Top 5 Countries):")
    print("-" * 70)
    
    continuity_issues = []
    
    for country in sample_countries:
        country_data = df[df['Country'] == country]
        years = sorted(country_data['Year'].unique())
        
        if len(years) > 0:
            year_span = years[-1] - years[0] + 1
            actual_years = len(years)
            continuity = (actual_years / year_span) * 100 if year_span > 0 else 0
            
            print(f"\n{country}:")
            print(f"   Year range: {years[0]} - {years[-1]}")
            print(f"   Years with data: {actual_years}/{year_span} ({continuity:.1f}%)")
            
            if continuity < 50:
                continuity_issues.append(f"{country}: {continuity:.1f}%")
                print(f"   ⚠️ Gaps in data continuity")
            else:
                print(f"   ✅ Good continuity")
    
    if continuity_issues:
        results['issues'].append(f"Continuity gaps in: {', '.join(continuity_issues[:3])}")
        print(f"\n⚠️ Some countries have gaps in year coverage")
    else:
        print(f"\n✅ Good data continuity across countries")
    
    return results


# ================================================================
# 🎯 FINAL DECISION FUNCTION
# ================================================================

def make_final_decision(all_results):
    """
    Make final pass/fail decision based on all validation results.
    
    Args:
        all_results (list): List of validation result dictionaries
    
    Returns:
        bool: True if data is ready, False otherwise
    """
    print("\n" + "="*70)
    print("🎯 FINAL VALIDATION DECISION")
    print("="*70)
    
    # Collect all issues
    all_issues = []
    critical_issues = []
    
    for result in all_results:
        if not result['passed']:
            critical_issues.extend(result['issues'])
        else:
            all_issues.extend(result['issues'])
    
    # Count checks
    total_checks = len(all_results)
    passed_checks = sum(1 for r in all_results if r['passed'])
    
    print(f"\n📊 Validation Summary:")
    print(f"   Total checks: {total_checks}")
    print(f"   Passed: {passed_checks}")
    print(f"   Failed: {total_checks - passed_checks}")
    
    # Print issues
    if critical_issues:
        print(f"\n❌ CRITICAL ISSUES ({len(critical_issues)}):")
        for issue in critical_issues:
            print(f"   • {issue}")
    
    if all_issues and not critical_issues:
        print(f"\n⚠️ MINOR ISSUES ({len(all_issues)}):")
        for issue in all_issues[:5]:  # Show first 5
            print(f"   • {issue}")
        if len(all_issues) > 5:
            print(f"   ... and {len(all_issues) - 5} more")
    
    # Make decision
    print("\n" + "="*70)
    
    if critical_issues:
        print("❌ DATA NEEDS FIXES")
        print("="*70)
        print("\n🔧 Required fixes:")
        for i, issue in enumerate(critical_issues[:5], 1):
            print(f"   {i}. {issue}")
        if len(critical_issues) > 5:
            print(f"   ... and {len(critical_issues) - 5} more issues")
        
        print("\n📝 Action required:")
        print("   Please fix the critical issues above before proceeding.")
        print("   Run the appropriate cleaning script to resolve these issues.")
        
        return False
    
    elif len(all_issues) > 3:
        print("⚠️ DATA HAS MINOR ISSUES")
        print("="*70)
        print("\nData is mostly ready but has some minor quality concerns.")
        print("Consider addressing these issues for better analysis quality.")
        
        return True
    
    else:
        print("✅ DATA IS CLEAN — READY FOR FEATURE ENGINEERING")
        print("="*70)
        print("\n🎉 Validation passed!")
        print("   ✓ No critical issues found")
        print("   ✓ Data quality is acceptable")
        print("   ✓ Ready for next step: Feature Engineering")
        
        return True


# ================================================================
# 💾 SAVE VALIDATION REPORT
# ================================================================

def save_validation_report(all_results, is_ready, output_path=None):
    """
    Save validation report to file.
    
    Args:
        all_results (list): List of validation results
        is_ready (bool): Final decision
        output_path (str): Optional output file path
    """
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Use encoding='utf-8' to handle emoji characters
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("TRADE DATA VALIDATION REPORT\n")
            f.write("="*70 + "\n\n")
            
            status = "PASSED" if is_ready else "FAILED"
            f.write(f"Status: {status}\n\n")
            
            for i, result in enumerate(all_results, 1):
                check_status = "PASS" if result['passed'] else "FAIL"
                f.write(f"Check {i}: {check_status}\n")
                if result['issues']:
                    for issue in result['issues']:
                        f.write(f"   - {issue}\n")
                f.write("\n")
        
        print(f"\n📄 Validation report saved to: {output_path}")


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """
    Main validation function - runs all checks and provides decision.
    """
    print("\n" + "="*70)
    print("✅ TRADE DATA VALIDATION")
    print("="*70)
    
    # Load input file
    print(f"\n📂 Loading: {INPUT_FILE}")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: File not found at {INPUT_FILE}")
        print(f"   Please ensure cleaned_trade.csv exists first!")
        sys.exit(1)
    
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"   ✓ Loaded successfully")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        sys.exit(1)
    
    # Run all validation checks
    print("\n" + "="*70)
    print("🔍 RUNNING VALIDATION CHECKS")
    print("="*70)
    
    all_results = []
    
    # Check 1: Basic Structure
    all_results.append(check_basic_structure(df))
    
    # Check 2: Missing Values
    all_results.append(check_missing_values(df))
    
    # Check 3: Country Validation
    all_results.append(validate_country_column(df))
    
    # Check 4: Year Validation
    all_results.append(validate_year_column(df))
    
    # Check 5: Trade Value Validation
    all_results.append(validate_trade_value(df))
    
    # Check 6: Trade Type Validation
    all_results.append(validate_trade_type(df))
    
    # Check 7: Partner Validation
    all_results.append(validate_partner_column(df))
    
    # Check 8: Duplicate Check
    all_results.append(check_duplicates(df))
    
    # Check 9: Data Distribution
    all_results.append(check_data_distribution(df))
    
    # Make final decision
    is_ready = make_final_decision(all_results)
    
    # Save report (optional)
    report_path = os.path.join(os.path.dirname(INPUT_FILE), "validation_report.txt")
    save_validation_report(all_results, is_ready, report_path)
    
    # Exit with appropriate code
    sys.exit(0 if is_ready else 1)


if __name__ == "__main__":
    main()
