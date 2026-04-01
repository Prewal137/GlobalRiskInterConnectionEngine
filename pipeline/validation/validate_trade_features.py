"""
🔍 Quick Validation of Trade Features Dataset

Performs final sanity checks before ML training.
"""

import pandas as pd
import numpy as np
import os

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/trade_features.csv")

# ================================================================
# 🎯 VALIDATION FUNCTIONS
# ================================================================

def load_and_check_data(input_path):
    """Load data and perform basic checks."""
    print("\n" + "="*70)
    print("📂 LOADING DATA")
    print("="*70)
    
    df = pd.read_csv(input_path)
    
    print(f"✓ Loaded: {len(df):,} rows, {len(df.columns)} columns")
    print(f"\n📋 Column Names:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i}. {col}")
    
    return df


def check_data_types(df):
    """Check data types are correct."""
    print("\n" + "="*70)
    print("✅ CHECK 1: DATA TYPES")
    print("="*70)
    
    print(f"\n{df.dtypes}\n")
    
    issues = []
    
    # Check numeric columns
    numeric_cols = ['Export', 'Import', 'Trade_Balance', 'Total_Trade', 
                   'Growth', 'Rolling_Mean_3', 'Volatility_3', 
                   'Export_Growth', 'Import_Growth', 'Export_Share', 
                   'Import_Share', 'Balance_Ratio']
    
    for col in numeric_cols:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            issues.append(f"❌ {col} should be numeric but is {df[col].dtype}")
        elif col in df.columns:
            print(f"✓ {col}: {df[col].dtype}")
    
    # Check Shock column (should be 0/1)
    if 'Shock' in df.columns:
        unique_shocks = df['Shock'].unique()
        print(f"\n✓ Shock values: {sorted(unique_shocks)}")
        if set(unique_shocks).issubset({0, 1}):
            print("✓ Shock is binary (0/1)")
        else:
            issues.append(f"❌ Shock has invalid values: {unique_shocks}")
    
    if issues:
        print("\n⚠️ ISSUES FOUND:")
        for issue in issues:
            print(issue)
    else:
        print("\n✅ All data types correct")
    
    return len(issues) == 0


def check_null_values(df):
    """Check for null/NaN values."""
    print("\n" + "="*70)
    print("✅ CHECK 2: NULL VALUES")
    print("="*70)
    
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    
    print(f"\n{'Column':<25} {'Null Count':<15} {'Percent':<10}")
    print("-" * 50)
    
    critical_nulls = []
    
    for col in df.columns:
        count = null_counts[col]
        percent = (count / len(df)) * 100
        
        if count > 0:
            status = "⚠️" if percent < 20 else "❌"
            print(f"{col:<25} {count:<15,} {percent:<10.2f}% {status}")
            
            if percent >= 20:
                critical_nulls.append(col)
        else:
            print(f"{col:<25} {count:<15,} {percent:<10.2f}% ✅")
    
    print(f"\n📊 Total null values: {total_nulls:,}")
    
    if critical_nulls:
        print(f"\n⚠️ High null columns (>20%): {critical_nulls}")
        print("   These may need imputation or removal")
        return False, critical_nulls
    else:
        print("\n✅ No critical null value issues")
        return True, []


def check_infinite_values(df):
    """Check for infinite values."""
    print("\n" + "="*70)
    print("✅ CHECK 3: INFINITE VALUES")
    print("="*70)
    
    inf_count = 0
    inf_columns = []
    
    for col in df.select_dtypes(include=[np.number]).columns:
        inf_values = np.isinf(df[col]).sum()
        if inf_values > 0:
            print(f"❌ {col}: {inf_values:,} infinite values")
            inf_count += inf_values
            inf_columns.append(col)
    
    if inf_count == 0:
        print("✅ No infinite values found")
        return True
    else:
        print(f"\n⚠️ Total infinite values: {inf_count:,}")
        print(f"   Affected columns: {inf_columns}")
        return False


def check_statistics(df):
    """Print descriptive statistics."""
    print("\n" + "="*70)
    print("✅ CHECK 4: DESCRIPTIVE STATISTICS")
    print("="*70)
    
    stats = df.describe()
    print(f"\n{stats.to_string()}")
    
    # Check for weird values
    print("\n\n🔍 Checking for unusual patterns...")
    
    issues = []
    
    # Check if Export/Import are non-negative
    if (df['Export'] < 0).any():
        issues.append("❌ Negative Export values found")
    else:
        print("✓ All Export values ≥ 0")
    
    if (df['Import'] < 0).any():
        issues.append("❌ Negative Import values found")
    else:
        print("✓ All Import values ≥ 0")
    
    # Check Total_Trade
    if (df['Total_Trade'] <= 0).sum() > 0:
        zero_count = (df['Total_Trade'] <= 0).sum()
        issues.append(f"⚠️ {zero_count} records with Total_Trade ≤ 0")
    else:
        print("✓ All Total_Trade values > 0")
    
    # Check Share columns (should be between 0 and 1 typically)
    for share_col in ['Export_Share', 'Import_Share']:
        if share_col in df.columns:
            out_of_range = ((df[share_col] < 0) | (df[share_col] > 1)).sum()
            if out_of_range > 0:
                print(f"⚠️ {share_col}: {out_of_range} values outside [0,1] range")
            else:
                print(f"✓ {share_col}: All values in [0,1]")
    
    return len(issues) == 0, issues


def check_shock_distribution(df):
    """Check Shock distribution - VERY IMPORTANT!"""
    print("\n" + "="*70)
    print("✅ CHECK 5: SHOCK DISTRIBUTION (CRITICAL)")
    print("="*70)
    
    shock_counts = df['Shock'].value_counts()
    shock_total = len(df)
    
    print(f"\n📊 Shock Distribution:")
    print(f"{'Value':<10} {'Count':<15} {'Percent':<10}")
    print("-" * 35)
    
    for value in sorted(shock_counts.index):
        count = shock_counts[value]
        percent = (count / shock_total) * 100
        label = "No Shock" if value == 0 else "Shock Event"
        print(f"{value:<10} {count:<15,} {percent:<10.2f}% ({label})")
    
    # Calculate imbalance ratio
    shock_percent = (shock_counts.get(1, 0) / shock_total) * 100
    
    print(f"\n📈 Class Imbalance Analysis:")
    print(f"   Shock events: {shock_counts.get(1, 0):,} ({shock_percent:.2f}%)")
    print(f"   Normal periods: {shock_counts.get(0, 0):,} ({100-shock_percent:.2f}%)")
    print(f"   Ratio: {shock_counts.get(0, 0) / max(shock_counts.get(1, 1), 1):.1f}:1")
    
    # Assessment
    print(f"\n🎯 ML Readiness Assessment:")
    
    if shock_percent < 5:
        print("   ⚠️ HIGHLY IMBALANCED (< 5% shocks)")
        print("   → Model may struggle to learn shock patterns")
        print("   → Consider SMOTE or class weighting")
        return False, "highly_imbalanced"
    elif shock_percent < 15:
        print("   ⚠️ MODERATELY IMBALANCED (5-15% shocks)")
        print("   → Should learn OK with class weighting")
        print("   → Monitor precision/recall")
        return True, "moderately_imbalanced"
    elif shock_percent < 40:
        print("   ✅ GOOD BALANCE (15-40% shocks)")
        print("   → Dataset suitable for standard ML")
        print("   → No special handling needed")
        return True, "well_balanced"
    else:
        print("   ⚠️ UNUSUAL DISTRIBUTION (> 40% shocks)")
        print("   → Verify shock threshold (-30%)")
        print("   → May indicate data quality issue")
        return False, "unusual_distribution"


def check_feature_correlations(df):
    """Quick correlation check."""
    print("\n" + "="*70)
    print("✅ CHECK 6: FEATURE CORRELATIONS")
    print("="*70)
    
    # Select numeric columns
    numeric_df = df.select_dtypes(include=[np.number])
    
    # Calculate correlations with Shock
    if 'Shock' in df.columns:
        correlations = numeric_df.corr()['Shock'].drop('Shock').sort_values(ascending=False)
        
        print(f"\n📊 Top Correlations with Shock:")
        print(f"{'Feature':<25} {'Correlation':<15}")
        print("-" * 40)
        
        for feat, corr in correlations.head(5).items():
            print(f"{feat:<25} {corr:<15.4f}")
        
        print(f"\n📉 Bottom Correlations with Shock:")
        for feat, corr in correlations.tail(3).items():
            print(f"{feat:<25} {corr:<15.4f}")
        
        # Check for high correlations
        corr_matrix = numeric_df.corr()
        high_corr_pairs = []
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = abs(corr_matrix.iloc[i, j])
                if corr_val > 0.9:
                    high_corr_pairs.append((corr_matrix.columns[i], 
                                           corr_matrix.columns[j], 
                                           corr_val))
        
        if high_corr_pairs:
            print(f"\n⚠️ HIGH CORRELATIONS (> 0.9):")
            for feat1, feat2, corr in high_corr_pairs:
                print(f"   • {feat1} ↔ {feat2}: {corr:.3f}")
            print("   → Consider removing one of each pair")
        else:
            print("\n✅ No severe multicollinearity")


def final_assessment(data_types_ok, nulls_ok, no_inf, stats_ok, shock_ok, balance_status):
    """Provide final ML readiness assessment."""
    print("\n" + "="*70)
    print("🎯 FINAL ML READINESS ASSESSMENT")
    print("="*70)
    
    all_checks = [data_types_ok, nulls_ok, no_inf, stats_ok, shock_ok]
    
    print(f"\n{'Check':<35} {'Status':<15}")
    print("-" * 50)
    print(f"{'Data Types':<35} {'✅ PASS' if data_types_ok else '❌ FAIL':<15}")
    print(f"{'Null Values':<35} {'✅ PASS' if nulls_ok else '⚠️ WARNING':<15}")
    print(f"{'Infinite Values':<35} {'✅ PASS' if no_inf else '❌ FAIL':<15}")
    print(f"{'Statistics':<35} {'✅ PASS' if stats_ok else '⚠️ WARNING':<15}")
    print(f"{'Shock Distribution':<35} {'✅ PASS' if shock_ok else '⚠️ WARNING':<15}")
    
    passed = sum(all_checks)
    total = len(all_checks)
    
    print(f"\n📊 Summary: {passed}/{total} checks passed")
    
    if all(all_checks):
        print("\n✅ DATASET IS READY FOR ML TRAINING")
        print("   All critical checks passed")
        print("   Proceed to model training")
    elif passed >= 4:
        print("\n⚠️ DATASET IS MOSTLY READY")
        print("   Minor issues detected but trainable")
        print(f"   Balance status: {balance_status}")
        print("   Review warnings above before training")
    else:
        print("\n❌ DATASET NEEDS ATTENTION")
        print("   Multiple issues found")
        print("   Fix issues before training")
    
    return all(all_checks)


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Run all validation checks."""
    print("\n" + "="*70)
    print("🔍 TRADE FEATURES VALIDATION")
    print("="*70)
    
    # Load data
    df = load_and_check_data(INPUT_FILE)
    
    # Run checks
    data_types_ok = check_data_types(df)
    nulls_ok, _ = check_null_values(df)
    no_inf = check_infinite_values(df)
    stats_ok, _ = check_statistics(df)
    shock_ok, balance_status = check_shock_distribution(df)
    check_feature_correlations(df)
    
    # Final assessment
    final_assessment(data_types_ok, nulls_ok, no_inf, stats_ok, shock_ok, balance_status)
    
    print("\n" + "="*70)
    print("🎉 VALIDATION COMPLETE")
    print("="*70)


if __name__ == "__main__":
    main()
