"""
🧹 Remove NaN Rows from Trade Features

Creates a clean, NaN-free version for ML training.
"""

import pandas as pd
import os

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/trade_features.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "../../data/processed/trade/trade_features_clean.csv")


# ================================================================
# 🎯 CLEANING FUNCTION
# ================================================================

def remove_nan_rows():
    """
    Strategy: Drop high-NaN columns first, then drop remaining NaN rows.
    This preserves more data while ensuring clean training set.
    """
    
    print("\n" + "="*70)
    print("🧹 REMOVING NaN ROWS FROM TRADE FEATURES")
    print("="*70)
    
    # Load data
    print(f"\n📂 Loading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    
    print(f"   Original shape: {df.shape}")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    
    # Count NaN before
    total_nan_before = df.isnull().sum().sum()
    print(f"\n📊 Total NaN values before: {total_nan_before:,}")
    
    # Show NaN per column
    print(f"\n{'Column':<25} {'NaN Count':<15} {'Percent':<10} {'Action':<15}")
    print("-" * 70)
    nan_counts = df.isnull().sum()
    
    # Identify columns to drop (>50% NaN)
    cols_to_drop = []
    for col in df.columns:
        count = nan_counts[col]
        percent = (count / len(df)) * 100
        
        if percent > 50:
            action = "DROP COLUMN"
            cols_to_drop.append(col)
        elif count > 0:
            action = "KEEP"
        else:
            action = "✓ Clean"
        
        print(f"{col:<25} {count:<15,} {percent:<10.2f}% {action:<15}")
    
    # Drop high-NaN columns
    if cols_to_drop:
        print(f"\n🗑️ Dropping {len(cols_to_drop)} columns with >50% NaN:")
        for col in cols_to_drop:
            print(f"   • {col}")
        
        df_reduced = df.drop(columns=cols_to_drop)
        print(f"   Shape after dropping columns: {df_reduced.shape}")
    else:
        df_reduced = df
    
    # Now drop rows with any remaining NaN
    print(f"\n🔧 Dropping rows with remaining NaN values...")
    df_clean = df_reduced.dropna().reset_index(drop=True)
    
    # Count NaN after
    total_nan_after = df_clean.isnull().sum().sum()
    
    print(f"\n✅ Final shape: {df_clean.shape}")
    print(f"   Rows removed: {len(df) - len(df_clean):,}")
    print(f"   Rows remaining: {len(df_clean):,}")
    print(f"   Total NaN values after: {total_nan_after}")
    
    # Verify no NaN
    assert df_clean.isnull().sum().sum() == 0, "Still have NaN values!"
    print(f"\n✓ Verified: No NaN values in clean dataset")
    
    # Save clean dataset
    print(f"\n💾 Saving to: {OUTPUT_FILE}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    df_clean.to_csv(OUTPUT_FILE, index=False)
    
    file_size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    print(f"   File size: {file_size_mb:.2f} MB")
    print(f"   ✓ Saved successfully!")
    
    # Show final statistics
    print(f"\n" + "="*70)
    print("📊 CLEAN DATASET STATISTICS")
    print("="*70)
    
    print(f"\n📈 Dataset Size:")
    print(f"   Total rows: {len(df_clean):,}")
    print(f"   Total columns: {len(df_clean.columns)}")
    print(f"   Unique countries: {df_clean['Country'].nunique()}")
    print(f"   Year range: {df_clean['Year'].min()} - {df_clean['Year'].max()}")
    
    # Shock distribution
    shock_count = df_clean['Shock'].sum()
    shock_percent = (shock_count / len(df_clean)) * 100
    
    print(f"\n⚡ Shock Distribution:")
    print(f"   Shock events (1): {shock_count:,} ({shock_percent:.2f}%)")
    print(f"   Normal periods (0): {len(df_clean) - shock_count:,} ({100-shock_percent:.2f}%)")
    print(f"   Ratio: {(len(df_clean) - shock_count) / max(shock_count, 1):.1f}:1")
    
    # Feature list
    print(f"\n📋 All Features ({len(df_clean.columns)} columns):")
    for i, col in enumerate(df_clean.columns, 1):
        print(f"   {i}. {col}")
    
    # Sample data
    print(f"\n📋 Sample Data (First 5 rows):")
    print("-" * 70)
    print(df_clean.head(5).to_string(index=False))
    
    print("\n" + "="*70)
    print("✅ DATASET READY FOR ML TRAINING")
    print("="*70)
    print(f"\n✨ Clean, NaN-free, production-ready!")
    print(f"📁 Output: {OUTPUT_FILE}")
    print("="*70)
    
    return df_clean


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    remove_nan_rows()
