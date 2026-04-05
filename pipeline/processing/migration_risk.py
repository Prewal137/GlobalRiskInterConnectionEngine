"""
🌍 Migration Risk Calculator (Domain-Driven, No ML)

Creates migration risk as a derived time-series signal based on
demographic and migration dynamics.

Input:
    - data/processed/migration/final_migration.csv

Output:
    - data/processed/migration/migration_risk_output.csv

Method:
    Hybrid domain-driven approach with weighted feature combination.
    NO machine learning models used.
"""

import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "migration", "final_migration.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "migration", "migration_risk_output.csv")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ================================================================
# 🔧 MAIN RISK CALCULATION PIPELINE
# ================================================================

def calculate_migration_risk():
    """
    Calculate migration risk using domain-driven weighted features.
    
    Returns:
        pd.DataFrame: Final migration risk output
    """
    print("\n" + "="*80)
    print("🌍 MIGRATION RISK CALCULATION (Domain-Driven)")
    print("="*80)
    
    # ========================================
    # STEP 1: LOAD DATA
    # ========================================
    print("\n📥 STEP 1: LOADING DATA")
    print("-"*80)
    
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    
    df = pd.read_csv(INPUT_FILE)
    
    print(f"   ✅ Loaded {len(df)} rows")
    print(f"   📋 Columns: {list(df.columns)}")
    print(f"   📅 Year range: {df['Year'].min()} - {df['Year'].max()}")
    print(f"   🌍 Countries: {df['Country'].unique()}")
    
    # Sort by Country and Year
    df = df.sort_values(['Country', 'Year']).reset_index(drop=True)
    print(f"   ✅ Sorted by Country, Year")
    
    # ========================================
    # STEP 2: CREATE FEATURES
    # ========================================
    print("\n⚙️  STEP 2: CREATING FEATURES")
    print("-"*80)
    
    # Feature 1: Migration change (percentage change, more stable than absolute diff)
    df['migration_change'] = df.groupby('Country')['Migration'].pct_change()
    print(f"   ✅ Created: migration_change (pct_change of Migration)")
    print(f"      Note: Using percentage change for scale-invariance")
    
    # Features 2-4: Already exist in dataset
    # - PopulationGrowth
    # - UrbanGrowth
    # - Unemployment
    
    print(f"   ✅ Using existing: PopulationGrowth, UrbanGrowth, Unemployment")
    
    # ========================================
    # STEP 3: HANDLE MISSING VALUES
    # ========================================
    print("\n🧹 STEP 3: HANDLING MISSING VALUES")
    print("-"*80)
    
    # Count missing before
    missing_before = df.isna().sum()
    print(f"   Missing values before cleaning:")
    for col, count in missing_before.items():
        if count > 0:
            print(f"      {col:25s}: {count}")
    
    # Drop first row per country (due to diff creating NaN)
    initial_rows = len(df)
    df = df.dropna(subset=['migration_change']).reset_index(drop=True)
    dropped_rows = initial_rows - len(df)
    
    if dropped_rows > 0:
        print(f"   ✅ Dropped {dropped_rows} rows (first year per country due to diff)")
    
    # Interpolate any remaining missing values
    numeric_cols = ['migration_change', 'PopulationGrowth', 'UrbanGrowth', 'Unemployment']
    
    for col in numeric_cols:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            print(f"      Interpolating {missing_count} missing values in {col}...")
            df[col] = df[col].interpolate(method='linear')
            df[col] = df[col].ffill().bfill()  # Handle edge cases
    
    # Verify no missing values remain
    missing_after = df[numeric_cols].isna().sum().sum()
    if missing_after == 0:
        print(f"   ✅ All missing values handled")
    else:
        print(f"   ⚠️  Warning: {missing_after} missing values remain")
    
    # ========================================
    # STEP 4: NORMALIZE FEATURES
    # ========================================
    print("\n📏 STEP 4: NORMALIZING FEATURES (MinMax 0-1)")
    print("-"*80)
    
    # Features to normalize
    features_to_normalize = [
        'migration_change',
        'PopulationGrowth',
        'UrbanGrowth',
        'Unemployment'
    ]
    
    # Fit scaler on entire dataset (no ML, so no leakage concern)
    scaler = MinMaxScaler(feature_range=(0, 1))
    df_normalized = df.copy()
    df_normalized[features_to_normalize] = scaler.fit_transform(df[features_to_normalize])
    
    print(f"   ✅ Normalized {len(features_to_normalize)} features to [0, 1]")
    
    # Show normalization ranges
    print(f"\n   Normalization ranges:")
    for col in features_to_normalize:
        print(f"      {col:25s}: [{df[col].min():8.2f}, {df[col].max():8.2f}] → [0.00, 1.00]")
    
    # ========================================
    # STEP 5: COMPUTE MIGRATION RISK
    # ========================================
    print("\n⚡ STEP 5: COMPUTING MIGRATION RISK")
    print("-"*80)
    
    # Domain-driven weights (heuristic based on migration literature)
    # 
    # Weight Justification:
    # - migration_change (0.4): Direct measure of migration flow volatility
    #   Highest weight as it captures immediate migration pressure
    # - PopulationGrowth (0.3): Demographic driver of future migration
    #   High population growth creates long-term migration pressure
    # - UrbanGrowth (0.2): Urbanization stress indicator
    #   Rapid urban growth signals rural-to-urban migration pressures
    # - Unemployment (0.1): Economic push factor
    #   Lower weight as unemployment affects migration indirectly
    #
    # Reference: Based on push-pull migration theory and demographic transition models
    
    weights = {
        'migration_change': 0.4,    # Direct migration pressure (highest)
        'PopulationGrowth': 0.3,    # Demographic driver (high)
        'UrbanGrowth': 0.2,         # Urbanization stress (moderate)
        'Unemployment': 0.1         # Economic push factor (lower)
    }
    
    print(f"   Formula:")
    print(f"      migration_risk =")
    for feature, weight in weights.items():
        print(f"         {weight:.1f} × {feature}_norm")
    
    # Calculate weighted sum
    df_normalized['migration_risk'] = (
        weights['migration_change'] * df_normalized['migration_change'] +
        weights['PopulationGrowth'] * df_normalized['PopulationGrowth'] +
        weights['UrbanGrowth'] * df_normalized['UrbanGrowth'] +
        weights['Unemployment'] * df_normalized['Unemployment']
    )
    
    print(f"\n   ✅ Migration risk calculated")
    print(f"   📊 Risk range: [{df_normalized['migration_risk'].min():.4f}, {df_normalized['migration_risk'].max():.4f}]")
    print(f"   📊 Mean risk: {df_normalized['migration_risk'].mean():.4f}")
    print(f"   📊 Std dev: {df_normalized['migration_risk'].std():.4f}")
    
    # ========================================
    # STEP 6: SMOOTH SIGNAL
    # ========================================
    print("\n🌊 STEP 6: SMOOTHING SIGNAL (Rolling Mean)")
    print("-"*80)
    
    window_size = 3
    df_normalized['migration_risk'] = df_normalized.groupby('Country')['migration_risk'].transform(
        lambda x: x.rolling(window=window_size, min_periods=1).mean()
    )
    
    print(f"   ✅ Applied rolling mean (window={window_size}, min_periods=1)")
    print(f"   📊 Smoothed risk range: [{df_normalized['migration_risk'].min():.4f}, {df_normalized['migration_risk'].max():.4f}]")
    
    # ========================================
    # STEP 7: CREATE FINAL STRUCTURE
    # ========================================
    print("\n📋 STEP 7: CREATING FINAL OUTPUT")
    print("-"*80)
    
    # Select only required columns
    final_columns = ['Country', 'Year', 'migration_risk']
    df_final = df_normalized[final_columns].copy()
    
    # Ensure correct data types
    df_final['Year'] = df_final['Year'].astype(int)
    df_final['migration_risk'] = df_final['migration_risk'].astype(float)
    
    print(f"   ✅ Final structure created")
    print(f"   📋 Columns: {list(df_final.columns)}")
    print(f"   📊 Rows: {len(df_final)}")
    
    # Preview
    print(f"\n📄 PREVIEW (first 10 rows):")
    print("-"*80)
    print(df_final.head(10).to_string(index=False))
    print("-"*80)
    
    # ========================================
    # STEP 8: SAVE OUTPUT
    # ========================================
    print("\n💾 STEP 8: SAVING OUTPUT")
    print("-"*80)
    
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   ✅ Saved to: {OUTPUT_FILE}")
    print(f"   📦 File size: {file_size:.2f} KB")
    print(f"   📊 Rows: {len(df_final)}")
    print(f"   📋 Columns: {len(df_final.columns)}")
    
    return df_final


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    try:
        df_risk = calculate_migration_risk()
        
        print("\n" + "="*80)
        print("✅ MIGRATION RISK CALCULATION COMPLETE")
        print("="*80)
        print(f"\n🎯 Key Insights:")
        print(f"   • Deterministic formula (no ML)")
        print(f"   • Domain-driven weights")
        print(f"   • Smoothed time-series signal")
        print(f"   • Ready for interconnection analysis")
        print(f"\n📁 Output: {OUTPUT_FILE}")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
