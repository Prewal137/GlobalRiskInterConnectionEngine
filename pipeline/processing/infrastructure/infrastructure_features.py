"""
🔧 Infrastructure Sector Feature Engineering (Phase 4)

Converts cleaned infrastructure data into ML-ready features.

Input:  data/processed/infrastructure/final_infrastructure.csv
Output: data/processed/infrastructure/infrastructure_features.csv

Features Created:
    - Lag features (t-1)
    - Growth rates (% change)
    - Rolling statistics (3-period mean)
    - Infrastructure pressure index
    - Normalized signals (0-1)
    - Composite risk target
"""

import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import warnings
warnings.filterwarnings('ignore')

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 3 levels from pipeline/processing/infrastructure/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "final_infrastructure.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "infrastructure_features.csv")

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

print("="*70)
print("🔧 INFRASTRUCTURE SECTOR FEATURE ENGINEERING")
print("="*70)
print(f"\n📂 Loading data from: {INPUT_FILE}")
print("-"*70)

# ================================================================
# 🎯 MAIN FEATURE ENGINEERING
# ================================================================

def main():
    """Main function to create infrastructure features."""
    
    # Step 1: Load data
    print("\n📥 Loading cleaned infrastructure data...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"\n❌ ERROR: Input file not found: {INPUT_FILE}")
        print(f"   Please run infrastructure_cleaner.py first.")
        return
    
    df = pd.read_csv(INPUT_FILE)
    print(f"   ✅ Loaded: {len(df)} rows, {len(df.columns)} columns")
    
    # Step 2: Rename columns to meaningful names
    print("\n" + "="*70)
    print("🏷️  STEP 1: RENAMING COLUMNS")
    print("="*70)
    
    column_mapping = {
        "percentage_of_households_with_source_of_drinking_water_uompercentage_scaling_factor1": "water_access",
        "revenue_earned_uominrindianrupees_scaling_factor10000000": "municipal_revenue",
        "annual_urban_population_at_mid_year_uomnumber_scaling_factor1000": "urban_population",
        "weighted_average_own_revenue_uominrindianrupees_scaling_factor10000000": "avg_revenue",
        "revenue_uominrindianrupees_scaling_factor10000000": "total_revenue",
        "percentage_of_households_uompercentage_scaling_factor1": "household_metric"
    }
    
    # Only rename columns that exist
    existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
    
    if existing_mapping:
        df = df.rename(columns=existing_mapping)
        print(f"   ✅ Renamed {len(existing_mapping)} columns:")
        for old, new in existing_mapping.items():
            print(f"      '{old[:50]}...' → '{new}'")
    else:
        print(f"   ⚠️  No columns matched mapping (already renamed?)")
    
    # Step 3: Sort data
    print("\n" + "="*70)
    print("📊 STEP 2: SORTING DATA")
    print("="*70)
    
    df = df.sort_values(["state", "year"]).reset_index(drop=True)
    print(f"   ✅ Sorted by state and year")
    print(f"   📅 Year range: {df['year'].min()} - {df['year'].max()}")
    print(f"   🗺️  States: {df['state'].nunique()}")
    
    # Step 4: Create lag features
    print("\n" + "="*70)
    print("⏮️  STEP 3: CREATING LAG FEATURES")
    print("="*70)
    
    lag_cols = ["water_access", "urban_population", "total_revenue"]
    
    for col in lag_cols:
        if col in df.columns:
            df[f"{col}_lag_1"] = df.groupby("state")[col].shift(1)
            print(f"   ✅ Created: {col}_lag_1")
    
    # Step 5: Create growth features
    print("\n" + "="*70)
    print("📈 STEP 4: CREATING GROWTH FEATURES")
    print("="*70)
    
    growth_cols = ["water_access", "urban_population", "total_revenue"]
    
    for col in growth_cols:
        if col in df.columns:
            df[f"{col}_growth"] = df.groupby("state")[col].pct_change()
            print(f"   ✅ Created: {col}_growth")
    
    # Step 6: Create rolling features
    print("\n" + "="*70)
    print("🔄 STEP 5: CREATING ROLLING FEATURES")
    print("="*70)
    
    roll_cols = ["water_access", "urban_population"]
    
    for col in roll_cols:
        if col in df.columns:
            df[f"{col}_roll_3"] = df.groupby("state")[col].rolling(3).mean().reset_index(0, drop=True)
            print(f"   ✅ Created: {col}_roll_3 (3-period mean)")
    
    # Step 7: Create infrastructure pressure index
    print("\n" + "="*70)
    print("⚡ STEP 6: CREATING INFRASTRUCTURE PRESSURE INDEX")
    print("="*70)
    
    if "urban_population" in df.columns and "water_access" in df.columns:
        df["infra_pressure"] = df["urban_population"] / (df["water_access"] + 1)
        print(f"   ✅ Created: infra_pressure")
        print(f"      Formula: urban_population / (water_access + 1)")
        print(f"      Range: [{df['infra_pressure'].min():.4f}, {df['infra_pressure'].max():.4f}]")
    
    # Step 8: Normalize features
    print("\n" + "="*70)
    print("📏 STEP 7: NORMALIZING FEATURES (0-1)")
    print("="*70)
    
    cols_to_scale = [
        "water_access",
        "urban_population",
        "total_revenue",
        "infra_pressure"
    ]
    
    # Only scale columns that exist
    existing_scale_cols = [col for col in cols_to_scale if col in df.columns]
    
    if existing_scale_cols:
        scaler = MinMaxScaler()
        df[existing_scale_cols] = scaler.fit_transform(df[existing_scale_cols])
        
        print(f"   ✅ Normalized {len(existing_scale_cols)} columns:")
        for col in existing_scale_cols:
            print(f"      {col:<25s}: [{df[col].min():.4f}, {df[col].max():.4f}]")
    else:
        print(f"   ⚠️  No columns to normalize")
    
    # Step 9: Create target variable (infrastructure risk)
    print("\n" + "="*70)
    print("🎯 STEP 8: CREATING TARGET VARIABLE")
    print("="*70)
    
    # Check if required columns exist
    required_for_target = ["water_access", "infra_pressure", "total_revenue"]
    missing_for_target = [col for col in required_for_target if col not in df.columns]
    
    if not missing_for_target:
        df["infrastructure_risk"] = (
            0.4 * (1 - df["water_access"]) +
            0.3 * df["infra_pressure"] +
            0.3 * (1 - df["total_revenue"])
        )
        
        print(f"   ✅ Created: infrastructure_risk")
        print(f"      Formula:")
        print(f"         0.4 × (1 - water_access)")
        print(f"       + 0.3 × infra_pressure")
        print(f"       + 0.3 × (1 - total_revenue)")
        print(f"      Range: [{df['infrastructure_risk'].min():.4f}, {df['infrastructure_risk'].max():.4f}]")
        print(f"      Mean: {df['infrastructure_risk'].mean():.4f}")
    else:
        print(f"   ⚠️  Cannot create target - missing columns: {missing_for_target}")
    
    # Step 10: Handle NaN values
    print("\n" + "="*70)
    print("🧹 STEP 9: HANDLING MISSING VALUES")
    print("="*70)
    
    before_count = len(df)
    df = df.dropna()
    after_count = len(df)
    removed = before_count - after_count
    
    print(f"   Rows before: {before_count}")
    print(f"   Rows after: {after_count}")
    print(f"   Removed: {removed} rows with NaN")
    
    if removed > 0:
        print(f"   ℹ️  Note: First year of each state loses lag/growth features")
    
    # Step 11: Save output
    print("\n" + "="*70)
    print("💾 STEP 10: SAVING OUTPUT")
    print("="*70)
    
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   ✅ Saved to: {OUTPUT_FILE}")
    print(f"   📦 File size: {file_size:.2f} KB")
    
    # Step 12: Print final summary
    print("\n" + "="*70)
    print("📊 FINAL DATASET SUMMARY")
    print("="*70)
    print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"   States: {df['state'].nunique()}")
    print(f"   Year range: {df['year'].min()} - {df['year'].max()}")
    print(f"   Missing values: {df.isna().sum().sum()}")
    
    # List all features created
    print(f"\n   📋 FEATURES CREATED:")
    print(f"   {'-'*100}")
    
    identifier_cols = ['country', 'state', 'year']
    target_col = ['infrastructure_risk'] if 'infrastructure_risk' in df.columns else []
    
    original_cols = [col for col in df.columns if col in ['water_access', 'urban_population', 
                                                           'total_revenue', 'municipal_revenue', 
                                                           'avg_revenue', 'household_metric']]
    
    lag_features = [col for col in df.columns if '_lag_' in col]
    growth_features = [col for col in df.columns if '_growth' in col]
    rolling_features = [col for col in df.columns if '_roll_' in col]
    engineered_features = [col for col in df.columns if col in ['infra_pressure']]
    
    print(f"      Identifiers ({len(identifier_cols)}): {', '.join(identifier_cols)}")
    print(f"      Original signals ({len(original_cols)}): {', '.join(original_cols)}")
    print(f"      Lag features ({len(lag_features)}): {', '.join(lag_features)}")
    print(f"      Growth features ({len(growth_features)}): {', '.join(growth_features)}")
    print(f"      Rolling features ({len(rolling_features)}): {', '.join(rolling_features)}")
    print(f"      Engineered features ({len(engineered_features)}): {', '.join(engineered_features)}")
    if target_col:
        print(f"      Target variable ({len(target_col)}): {', '.join(target_col)}")
    
    total_features = len(lag_features) + len(growth_features) + len(rolling_features) + len(engineered_features)
    print(f"\n      Total engineered features: {total_features}")
    
    # Show sample data
    print(f"\n   📋 SAMPLE DATA (first 5 rows):")
    print(f"   {'-'*100}")
    print(df.head().to_string(index=False))
    print(f"   {'-'*100}")
    
    # Column statistics
    if 'infrastructure_risk' in df.columns:
        print(f"\n   📈 TARGET VARIABLE STATISTICS:")
        print(f"      Mean: {df['infrastructure_risk'].mean():.4f}")
        print(f"      Std: {df['infrastructure_risk'].std():.4f}")
        print(f"      Min: {df['infrastructure_risk'].min():.4f}")
        print(f"      Max: {df['infrastructure_risk'].max():.4f}")
        print(f"      Median: {df['infrastructure_risk'].median():.4f}")
    
    print(f"\n" + "="*70)
    print(f"✅ INFRASTRUCTURE FEATURE ENGINEERING COMPLETE")
    print("="*70)
    print(f"\n📁 Next steps:")
    print(f"   1. Review output: {OUTPUT_FILE}")
    print(f"   2. Proceed to Phase 5: Model Training")
    print(f"   3. Create infrastructure_model.py")
    print(f"\n" + "="*70)


# ================================================================
# 🚀 RUN
# ================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
